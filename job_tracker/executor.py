from datetime import datetime
from time import sleep
from typing import Any, List
from redis import Redis
from torch import threshold
from queues.redisqueue import RedisQueue
from .worker import worker_fn

import requests
import multiprocessing
import threading
import json
import signal
import sys

broker = Redis("localhost")
queue = RedisQueue(broker, "jobqueue")

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
        for i in range(self.num_workers):
            print(f"Spawing Worker {i+1}")
            workers.append(multiprocessing.Process(target=target_fn, args=args))
        return workers

    def spawn_prefetch_threads(self, target_fn, args=()) -> List:
        threads = []
        for i in range(self.num_prefetch_threads):
            print(f"Spawing Thread {i+1}")
            threads.append(threading.Thread(target=target_fn, args=args))
        return threads

    def get_datetime(self) -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    def prefetch_fn(self):
        request_url = f"http://{self.fetch_ip}:{self.fetch_port}/{self.fetch_route}"
        r = requests.get(request_url, params={"prefetch_factor": self.prefetch_factor})
        if r.status_code != 200:
            print(f"Message : {json.loads(r.text)}")
            print(f"Status Code : {r.status_code}")
        else:
            print(f"[{self.get_datetime()}] Queued Submission in Job Queue | Current Queue Length : {len(queue)}")
        r.close()

    def global_queue_fn(self):
        joined = 0
        spawned = 0
        while not self.global_queue_thread.stopped():
            queue_length = len(queue)

            if queue_length < self.threshold * 2:
                if spawned and not joined:
                    for thread in self.prefetch_threads:
                        if thread.is_alive():
                            thread.join()
                    joined = 1
                    spawned = 0

                self.prefetch_threads = self.spawn_prefetch_threads(target_fn=self.prefetch_fn)
                for thread in self.prefetch_threads:
                    thread.start()
                
                joined = 0
                spawned = 1
            
            if queue_length >= self.threshold * 2 and not joined:
                for ix, thread in enumerate(self.prefetch_threads):
                    if thread.is_alive():
                        print(f"Thread-{ix+1}.join()")
                        thread.join()

                joined = 1

            sleep(2)
    
    def global_queue_cleanup(self) -> None:
        for ix, thread in enumerate(self.prefetch_threads):
            if thread.is_alive():
                print(f"Force Thread-{ix+1}.join()")
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
        
        for ix,workers in enumerate(self.workers):
            print(f"Terminating Worker {ix+1}")
            workers.terminate()
            workers.join()

        print(f"Terminating Queue Thread")
        self.global_queue_thread.join(timeout=5)

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
        threshold=20
    )

    print(executor.__dict__)

    docker_ip = "localhost"
    docker_port = 10000
    docker_route = "run_job"

    def signal_handler(sig, frame):
        executor.cleanup()
        print('You pressed Ctrl+C!')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)

    executor.execute(worker_fn, args=(docker_ip, docker_port, docker_route))