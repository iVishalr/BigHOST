import sys
import json
import signal
import requests
import threading
import multiprocessing

from time import sleep
from typing import Dict, List
from redis import Redis
from .worker import worker_fn
from datetime import datetime
from queues.redisqueue import RedisQueue

broker = Redis("localhost")
redis_queue = RedisQueue(broker, "jobqueue")

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
        """

        self.num_workers = num_workers
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

        if global_queue_thread == True and global_prefetch_thread == False:
            AssertionError(f"global_prefetch_thread needs to be True when global_queue_thread=True, but got global_prefetch_thread={global_prefetch_thread}.")
        if global_queue_thread == False and global_prefetch_thread == True:
            AssertionError(f"global_queue_thread needs to be True when global_prefetch_thread=True, but got global_queue_thread={global_queue_thread}.")

        if global_queue_thread == True and global_prefetch_thread == True:
            self.executor_mode = 0
            print(f"Maintaining a global queue thread and {self.num_prefetch_threads} prefetch threads.")
        else:
            print(f"Each worker will maintain its own queue thread and a prefetch thread.")

    def spawn_workers(self, target_fn, args=()) -> List:
        workers = []
        num_args = len(args)

        for i in range(self.num_workers):
            print(f"[{self.get_datetime()}] [master_p]\tSpawing Worker {i+1}")
            if len(args) < num_args+1:
                args = list(args)
                args = tuple([i+1] + args)
            else:
                args = list(args)
                args[0] = i+1
                args = tuple(args) # add ranks to processes
            workers.append(multiprocessing.Process(target=target_fn, args=args))
        return workers

    def spawn_prefetch_threads(self, target_fn, args=()) -> List:
        threads = []
        num_args = len(args)
        for i in range(self.num_prefetch_threads):
            print(f"[{self.get_datetime()}] [master_p]\tSpawing Thread {i+1}")
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
        if num_submissions == 0:
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

            sleep(queue_thread_timeout)
    
    def global_queue_cleanup(self) -> None:
        for ix, thread in enumerate(self.prefetch_threads):
            if thread.is_alive():
                print(f"[{self.get_datetime()}] [master_p]\tForce Thread-{ix+1}.join()")
                thread.join()

        self.global_queue_thread.stop()

    def global_execute(self, target_fn, args=None) -> None:
        # spawn a queue thread in master process
        self.global_queue_thread = QueueThread(target=self.global_queue_fn)
        self.global_queue_thread.start()
        # spawn multiple processes that read from the queue
        self.workers = self.spawn_workers(target_fn=target_fn, args=args)
        for workers in self.workers:
            workers.start()
    
    def global_cleanup(self) -> None:
        
        for ix, workers in enumerate(self.workers):
            print(f"[{self.get_datetime()}] [master_p]\tTerminating Worker {ix+1}")
            if workers.is_alive():
                workers.terminate()
                workers.join()
                workers.close()

        print(f"[{self.get_datetime()}] [master_p]\tTerminating Queue Thread")
        self.global_queue_cleanup()
        self.global_queue_thread.join(timeout=20)

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
        fetch_ip="localhost",
        fetch_port=9000,
        fetch_route="get-submissions",
        num_workers=1,
        global_queue_thread=True,
        global_prefetch_thread=True,
        prefetch_threads=4,
        prefetch_factor=4,
        threshold=5
    )

    print(executor.__dict__)

    docker_ip = "localhost"
    docker_port = 10000
    docker_route = "run_job"

    def signal_handler(sig, frame):
        executor.cleanup()
        print(f'[{executor.get_datetime()}] [master_p]\tEvaluator has been stopped.')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    executor.execute(worker_fn, args=(executor.team_dict, docker_ip, docker_port, docker_route))
    signal.pause()