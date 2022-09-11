import os
import sys
import json
import signal
import requests
import threading
import multiprocessing

from time import sleep
from typing import Dict, List

from output_processor import output
from .worker import worker_fn
from datetime import datetime

from job_tracker import broker, queue as redis_queue, BACKEND_INTERNAL_IP, BACKEND_EXTERNAL_IP


class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    def flush(self):
        pass

f = open('./logs.txt', 'w+')
backup = sys.stdout
sys.stdout = Tee(sys.stdout, f)

class QueueThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super(QueueThread, self).__init__(*args, **kwargs)
        self._stopper = threading.Event()

    def stop(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.is_set()

class ExecutorContext:
    
    def __init__(
        self, 
        fetch_ip : str = "0.0.0.0",
        fetch_port : int = 10000,
        fetch_route : str = "receive",
        num_workers : int = 4, 
        global_queue_thread : bool = True, 
        global_prefetch_thread : bool = False, 
        prefetch_threads: int = 2,
        prefetch_factor: int = 2,
        threshold: int = 16,
        num_backends: int = 8,
        ) -> None:

        """
        Arguments
        ----
        `fetch_ip` : str - IP Address of Website's backend.
        `fetch_port` : int - Port number of the Website's backend flask server.
        `fetch_route` : str - Route in flask server to submit the fetch requests.
        `num_workers` : int - Number of processes to spawn to read from redis queue. Default is 4.
        `global_queue_thread` : bool - If True, a single thread is maintained for checking queue length. Default is False.
        `global_prefetch_thread` : bool - If True, threads will be spawned on the master process for fetching new submissions. If false, each worker will maintain a thread that will fetch new submissions.
        `prefetch_threads` : int - This option is used only if `global_prefetch_thread = True`. Specifies the number of threads to fetch new submissions. 
        `prefetch_factor` : int - Number of submissions to be returned per request.
        `threshold` : int - The threshold for starting to fetch new submissions. Threshold indicates a certain queue length. 
        `num_backends` : int - Specifies the number of backend workers.
        """

        self.num_workers = num_workers
        self.num_backends = num_backends
        self.global_queue_thread = global_queue_thread
        self.global_prefetch_thread = global_prefetch_thread
        self.num_prefetch_threads = prefetch_threads
        self.prefetch_factor = prefetch_factor
        self.threshold = threshold

        self.fetch_ip = fetch_ip
        self.fetch_port = fetch_port
        self.fetch_route = fetch_route

        self.executor_mode = 1

        self.workers : List[multiprocessing.Process] = []
        self.prefetch_threads : List[threading.Thread] = []
        self.queue_thread : QueueThread = None
        self.thread_res = threading.Event()
        self.manager = multiprocessing.Manager()
        self.team_dict : Dict[str: int] = self.manager.dict()
        self.blacklist_queue = self.manager.Queue()

        assert num_backends % num_workers == 0, "num_backends must be completely divisible by num_workers"

        self.num_threads = num_backends // num_workers

        if global_queue_thread == True and global_prefetch_thread == False:
            AssertionError(f"global_prefetch_thread needs to be True when global_queue_thread=True, but got global_prefetch_thread={global_prefetch_thread}.")
        if global_queue_thread == False and global_prefetch_thread == True:
            AssertionError(f"global_queue_thread needs to be True when global_prefetch_thread=True, but got global_queue_thread={global_queue_thread}.")

        if global_queue_thread == True and global_prefetch_thread == True:
            self.executor_mode = 0
            self.executor_mode_desc = f"Maintaining a global queue thread and {self.num_prefetch_threads} prefetch threads."
        else:
            self.executor_mode_desc = f"Each worker will maintain its own queue thread and a prefetch thread. (NOT IMPLEMENTED)"

    def __str__(self) -> str:
        buffer = []
        buffer.append(f"Executor Context Configuration")
        buffer.append(f"------------------------------")
        buffer.append(f"fetch_ip : IP Address of Website's backend = {self.fetch_ip}")
        buffer.append(f"fetch_port : Website's backend port = {self.fetch_port}")
        buffer.append(f"fetch_route : Route on website's backend to get submissions = {self.fetch_route}")
        buffer.append(f"global_queue_thread : Maintains a thread in master process to monitor len(queue) = {self.global_queue_thread}")
        buffer.append(f"global_prefetch_thread : Spawns prefetch_threads in master process = {self.global_prefetch_thread}")
        buffer.append(f"num_workers : Number of producers reading from Job Queue = {self.num_workers}")
        buffer.append(f"num_prefetch_threads : Number of prefetch_threads to spawn for fetching submissions = {self.num_prefetch_threads}")
        buffer.append(f"threshold : Thershold for fetching next batch of submissions = {self.threshold}")
        buffer.append(f"executor_mode : ExecutorContext running mode = {self.executor_mode}")
        buffer.append(f"executor_mode_desc : ExecutorContext Mode Description = {self.executor_mode_desc}")
        buffer.append(f"workers : Buffer for maintaining pointers to worker processes = {self.workers}")
        buffer.append(f"prefetch_threads : Buffer for maintaining pointers to prefetch_threads = {self.prefetch_threads}")
        buffer.append(f"queue_thread : Pointer to global queue thread = {self.queue_thread}")
        buffer.append(f"thread_res : threading.Event() for prefetch_threads = {self.thread_res}")
        buffer.append(f"manager : multiprocessing.Manager() object = {self.manager}")
        buffer.append(f"team_dict : Shared dictionary for tracking dangerous submissions = {self.team_dict}")
        buffer.append("\n")
        return "\n".join(buffer)

    def signal_handler(self,sig, frame):
        self.cleanup()
        print(f'[{executor.get_datetime()}] [master_p]\tEvaluator has been stopped.')
        sys.exit(0)

    def spawn_workers(self, target_fn, args=()) -> List:
        workers = []
        num_args = len(args)

        for i in range(self.num_workers):
            print(f"[{self.get_datetime()}] [master_p]\tSpawning Worker {i+1}")
            if len(args) < num_args+1:
                args = list(args)
                args = tuple([i+1] + args)
            else:
                args = list(args)
                args[0] = i+1
                args = tuple(args) # add ranks to processes
            workers.append(multiprocessing.Process(target=target_fn, args=args, daemon=True))
        assert len(workers) == self.num_workers, "Unable to start all workers"
        return workers

    def spawn_prefetch_threads(self, target_fn, args=()) -> List:
        threads = []
        num_args = len(args)
        for i in range(self.num_prefetch_threads):
            print(f"[{self.get_datetime()}] [master_p]\tSpawning Thread {i+1}")
            if len(args) < num_args+1:
                args = tuple([i+1]+list(args))
            else:
                args = list(args)
                args[0] = i+1
                args = tuple(args)
            threads.append(threading.Thread(target=target_fn, args=args))
        return threads

    def get_datetime(self) -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    def prefetch_fn(self, rank):
        num_submissions = -1
        request_url = f"http://{self.fetch_ip}:{self.fetch_port}/{self.fetch_route}"
        r = requests.get(request_url, params={"prefetch_factor": self.prefetch_factor})
        if r.status_code != 200:
            print(f"Message : {json.loads(r.text)}")
            print(f"Status Code : {r.status_code}")
        else:
            res = json.loads(r.text)
            num_submissions = res["num_submissions"]
            if num_submissions != 0: 
                print(f"[{self.get_datetime()}] [prefet_{rank}]\tQueued {num_submissions} Submissions in Job Queue | Current Queue Length : {len(redis_queue)}")
            else:
                print(f"[{self.get_datetime()}] [prefet_{rank}]\tNo more submissions to fetch | Current Queue Length : {len(redis_queue)}")
        r.close()
        if num_submissions < self.threshold:
            self.thread_res.set()
        else:
            self.thread_res.clear()

    def global_queue_fn(self):
        joined = 0
        spawned = 0
        timeout = 0.15
        queue_thread_timeout = 2
        queue_trottled = 0

        initial_prefetch_threads = self.num_prefetch_threads

        while not self.global_queue_thread.stopped():
            queue_length = len(redis_queue)

            if queue_length < self.threshold * 2:

                self.prefetch_threads = self.spawn_prefetch_threads(target_fn=self.prefetch_fn)
                for thread in self.prefetch_threads:
                    thread.start()
                
                joined = 0
                spawned = 1
            
            if spawned and not joined:
                for ix, thread in enumerate(self.prefetch_threads):
                    if thread.is_alive():
                        thread.join()

                    if self.thread_res.is_set():
                        timeout += 0.5
                        queue_thread_timeout += timeout
                        
                        if queue_thread_timeout > 60:
                            queue_thread_timeout = 60

                        queue_trottled = 1
                        print(f"[{self.get_datetime()}] [queue_mt]\tIncreasing Queue Thread Timeout to {queue_thread_timeout:.04f}s.")
                    else:
                        if queue_trottled:
                            print(f"[{self.get_datetime()}] [queue_mt]\tResetting Queue Threads | Setting prefetch_threads to {initial_prefetch_threads}.")
                        timeout = 0.15
                        queue_thread_timeout = 2
                        queue_trottled = 0
                        self.num_prefetch_threads = initial_prefetch_threads
                        self.thread_res.clear()
                
                joined = 1
                spawned = 0

            if (joined and queue_trottled) and self.num_prefetch_threads > 1:
                print(f"[{self.get_datetime()}] [queue_mt]\tDown throttling Queue Thread | Setting prefetch_threads to 1.")
                self.num_prefetch_threads = 1

            # print(f"[{self.get_datetime()}] [queue_mt]\tSleeping {queue_thread_timeout:.04f}s.")
            sleep(queue_thread_timeout)
    
    def global_queue_cleanup(self) -> None:
        for ix, thread in enumerate(self.prefetch_threads):
            if thread.is_alive():
                print(f"[{self.get_datetime()}] [master_p]\tForce Thread-{ix+1}.join()")
                thread.join()

        self.global_queue_thread.stop()

    def global_execute(self, target_fn, args=None) -> None:
        # spawn a queue thread in master process
        for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGCHLD]:
            signal.signal(signame, signal.SIG_DFL)

        self.global_queue_thread = QueueThread(target=self.global_queue_fn)
        self.global_queue_thread.start()

        sleep(5)
        # spawn multiple processes that read from the queue
        self.workers = self.spawn_workers(target_fn=target_fn, args=args)
        for workers in self.workers:
            workers.start()

        for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGCHLD]:
            signal.signal(signame, self.signal_handler)
    
    def global_cleanup(self) -> None:
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        for ix, workers in enumerate(self.workers):
            print(f"[{self.get_datetime()}] [master_p]\tTerminating Worker {ix+1}")
            if workers.is_alive():
                workers.join()

        print(f"[{self.get_datetime()}] [master_p]\tTerminating Queue Thread")
        self.global_queue_cleanup()
        self.global_queue_thread.join(timeout=20)
        broker.close()

    def local_execute(self, target_fn):
        pass

    def local_cleanup(self):
        pass

    def execute(self, target_fn, args=None):
        if self.executor_mode == 0:
            self.global_execute(target_fn, args)
        else:
            self.local_execute(target_fn)
        return

    def cleanup(self):
        if self.executor_mode == 0:
            self.global_cleanup()
        else:
            self.local_cleanup()   

if __name__ == "__main__":
    
    executor = ExecutorContext(
        fetch_ip=BACKEND_INTERNAL_IP,
        fetch_port=9000,
        fetch_route="get-jobs",
        num_workers=1,
        global_queue_thread=True,
        global_prefetch_thread=True,
        prefetch_threads=4,
        prefetch_factor=4,
        threshold=30,
        num_backends=1
    )

    print(executor)

    docker_ip = "localhost"
    docker_port = 10000
    docker_route = "run_job"
    docker_image = "hadoop-3.2.2:0.1"

    backend_cpu_limit: int = 3
    backend_mem_limit: str = "6000m"
    backend_host_output_dir: str = f"{os.path.join(os.getcwd(),'output')}"
    backend_docker_output_dir: str = f"/output"
    backend_memswapiness: int = 0

    executor.execute(worker_fn, args=(executor.team_dict, docker_ip, docker_port, docker_route, docker_image, executor.num_threads, backend_cpu_limit, backend_mem_limit, backend_memswapiness, backend_host_output_dir, backend_docker_output_dir,))
    signal.pause()