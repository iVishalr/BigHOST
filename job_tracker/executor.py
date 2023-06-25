import os
import sys
import json
import uuid
import signal
import requests
import threading
import multiprocessing

from time import sleep
from common.utils import Tee, Logger
from typing import Dict, List
from datetime import datetime

from job_tracker import broker, queue as redis_queue, BACKEND_INTERNAL_IP
import platform,socket,re,psutil,logging
from pprint import pprint

# f = open('./logs.txt', 'w+')
# backup = sys.stdout
# sys.stdout = Tee(sys.stdout, f)

sys.stdout = Logger("./logs.txt", "a+")

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
        name: str = None, 
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
        timeout: int = 60,
        log_dir: str = "logs",
        log_timeout: int = 120,
        sys_timeout: int = 1,
        output_processor_threads: int = 1,
        output_processor_timeout: int = 30,
        store_lats: bool = True,
        lats_dir: str = "lats"
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
        self.timeout = timeout

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
        self.running_dict : Dict[str: int] = self.manager.dict()
        self.blacklist_queue = self.manager.Queue()
        self.lock = self.manager.Lock()
        
        self.output_processor_event = threading.Event()
        self.output_processor_threads = output_processor_threads
        self.output_processor_timeout = output_processor_timeout
        self.store_lats = store_lats
        self.lats_dir = lats_dir
        self.output_processor_args = None
        self.worker_args = None

        self.executor_name: str = name
        self.executor_uuid: str = str(uuid.uuid4())
        self.log_dir = log_dir
        self.log_timeout = log_timeout
        self.sys_timeout = sys_timeout

        assert num_backends % num_workers == 0, "num_backends must be completely divisible by num_workers"

        self.num_threads = num_backends // num_workers

        if global_queue_thread == True and global_prefetch_thread == False:
            AssertionError(f"global_prefetch_thread needs to be True when global_queue_thread=True, but got global_prefetch_thread={global_prefetch_thread}.")
        if global_queue_thread == False and global_prefetch_thread == True:
            AssertionError(f"global_queue_thread needs to be True when global_prefetch_thread=True, but got global_queue_thread={global_queue_thread}.")

        # create log directories
        self.log_path = os.path.join(self.log_dir, self.executor_name, self.executor_uuid)
        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

        self.lats_path = os.path.join(self.lats_dir, self.executor_name, self.executor_uuid)
        if store_lats and not os.path.exists(self.lats_path):
            os.makedirs(self.lats_path)

        if global_queue_thread == True and global_prefetch_thread == True:
            self.executor_mode = 0
            self.executor_mode_desc = f"Maintaining a global queue thread and {self.num_prefetch_threads} prefetch threads."
        else:
            self.executor_mode_desc = f"Each worker will maintain its own queue thread and a prefetch thread. (NOT IMPLEMENTED)"

        if not self.register_executor():
            exit(1)

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
        buffer.append(f"timeout : Timeout for queue_threads and workers = {self.timeout}")
        buffer.append(f"executor_mode : ExecutorContext running mode = {self.executor_mode}")
        buffer.append(f"executor_mode_desc : ExecutorContext Mode Description = {self.executor_mode_desc}")
        buffer.append(f"workers : Buffer for maintaining pointers to worker processes = {self.workers}")
        buffer.append(f"prefetch_threads : Buffer for maintaining pointers to prefetch_threads = {self.prefetch_threads}")
        buffer.append(f"queue_thread : Pointer to global queue thread = {self.queue_thread}")
        buffer.append(f"thread_res : threading.Event() for prefetch_threads = {self.thread_res}")
        buffer.append(f"manager : multiprocessing.Manager() object = {self.manager}")
        buffer.append(f"team_dict : Shared dictionary for tracking dangerous submissions = {self.team_dict}")
        buffer.append(f"running_dict : Shared dictionary for tracking submissions running on different workers = {self.running_dict}")
        buffer.append("\n")
        return "\n".join(buffer)

    def signal_handler(self, sig, frame):
        self.cleanup()
        print(f'[{executor.get_datetime()}] [master_p]\tEvaluator has been stopped.')
        sys.exit(0)

    def poll_cpu(self):
        
        """
        Fetch current CPU, RAM and Swap utilisation
        Returns
        -------
        float
            CPU utilisation (percentage)
        float
            RAM utilisation (percentage)
        float
            Swap utilisation (percentage)
        """

        return (
            psutil.cpu_percent(),
            psutil.virtual_memory().percent,
            psutil.swap_memory().percent,
        )

    def get_sys_report(self):
        try:
            info={}
            info['platform']=platform.system()
            info['platform-release']=platform.release()
            info['platform-version']=platform.version()
            info['architecture']=platform.machine()
            info['hostname']=socket.gethostname()
            info['ip-address']=socket.gethostbyname(socket.gethostname())
            info['mac-address']=':'.join(re.findall('..', '%012x' % uuid.getnode()))
            info['processor']=platform.processor()
            info['ram']=str(round(psutil.virtual_memory().total / (1024.0 **3)))+" GB"
            return info
        except Exception as e:
            logging.exception(e)

    def register_executor(self) -> bool:
        payload = {
            'executor_name': self.executor_name,
            'executor_uuid': self.executor_uuid,
            'executor_log_dir': self.log_dir,
            'num_backends': self.num_backends,
            'num_workers': self.num_workers,
            'num_threads': self.num_threads,
            'num_prefetch_threads': self.num_prefetch_threads,
            'prefetch_factor': self.prefetch_factor,
            'threshold': self.threshold,
            'timeout': self.timeout,
            'cpu_limit': backend_cpu_limit,
            'mem_limit': backend_mem_limit,
            'sys_info': self.get_sys_report()
        }
        
        url = f"http://{self.fetch_ip}:{self.fetch_port}/register_executor"
        r = requests.post(url, data = json.dumps(payload))
        res = json.loads(r.text)
        r.close()

        if int(res["status"]) == 200:
            # print(f"[{self.get_datetime()}]\tExecutor registered with backend server.")
            return True
        else:
            print(f"[{self.get_datetime()}]\tFailed to register this executor with backend server.")
            return False

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

    def read_logs(self, path, offset):
        f = open(path, "r")
        f.seek(offset)
        data = f.read()
        new_offset = f.tell()
        f.close()
        return data, new_offset

    def get_worker_logs(self, path, offset):
        logs = {}
        new_offset = 0
        for filename in os.listdir(path):
            if ".txt" not in filename or 'worker' not in filename:
                continue
            data, new_offset = self.read_logs(os.path.join(path, filename), offset)
            logs[filename] = data
        return logs, new_offset

    def get_op_logs(self, path, offset):
        logs = {}
        new_offset = 0
        for filename in os.listdir(path):
            if ".txt" not in filename or 'output_processor' not in filename:
                continue
            data, new_offset = self.read_logs(os.path.join(path, filename), offset)
            logs[filename] = data
        return logs, new_offset

    def get_lats_logs(self, path, offset):
        logs = {}
        new_offset = 0
        for filename in os.listdir(path):
            if ".txt" not in filename or 'lats' not in filename:
                continue
            data, new_offset = self.read_logs(os.path.join(path, filename), offset)
            logs[filename] = data
        return logs, new_offset

    def get_sys_logs(self, path, offset):
        logs = {}
        new_offset = 0
        for filename in os.listdir(path):
            if ".txt" not in filename or 'sys' not in filename:
                continue
            data, new_offset = self.read_logs(os.path.join(path, filename), offset)
            logs[filename] = data
        return logs, new_offset

    def sys_fn(self):
        print(f"[{self.get_datetime()}] [sys thread]\tStarting.")
        timeout = self.sys_timeout
        log_timeout = self.log_timeout
        executor_log_path = os.path.join(self.log_dir, self.executor_name, self.executor_uuid)
        f = open(os.path.join(executor_log_path, "sys_logs.txt"), "a+")
        
        avg_util = {
            "CPU": {
                'sum': 0.0,
                'values': [],
                'avg': 0.0
            }, 
            "Memory": {
                'sum': 0.0,
                'values': [],
                'avg': 0.0
            }, 
            "Swap": {
                'sum': 0.0,
                'values': [],
                'avg': 0.0
            }
        }

        time_slept = 0
        while not self.global_queue_thread.stopped():
            stats = self.poll_cpu()
            record = f"CPU: {stats[0]:.2f}% | Memory: {stats[1]:.2f}% | Swap: {stats[2]:.2f}%\n"

            for ix, category in enumerate(avg_util.keys()):
                avg_util[category]["sum"] += stats[ix]
                avg_util[category]["values"].append(stats[ix])
                if len(avg_util[category]["values"]) > (log_timeout // timeout):
                    avg_util[category]["sum"] -= avg_util[category]["values"].pop(0)
                avg_util[category]["avg"] = avg_util[category]["sum"] / len(avg_util[category]["values"])

            f.write(record)
            time_slept += timeout
            sleep(timeout)
            if time_slept % log_timeout == 0:
                print(f"[{self.get_datetime()}] [sys thread]\tAverage Utilization over {log_timeout}s - CPU: {avg_util['CPU']['avg']:.2f}% | Memory: {avg_util['Memory']['avg']:.2f}% | Swap: {avg_util['Swap']['avg']:.2f}%.")
                time_slept = 0
                f.flush()

        f.close()
        print(f"[{self.get_datetime()}] [sys thread]\tStopped.")

    def logs_fn(self):
        print(f"[{self.get_datetime()}] [log thread]\tStarting.")
        timeout = self.log_timeout
        url = f"http://{self.fetch_ip}:{self.fetch_port}/executor-log"
        executor_log_path = os.path.join(self.log_dir, self.executor_name, self.executor_uuid)
        
        wlog_offset, oplog_offset, latslog_offset, sys_log_offset = 0, 0, 0, 0
        while not self.global_queue_thread.stopped():
            wlogs, wlog_offset = self.get_worker_logs(executor_log_path, wlog_offset)
            oplogs, oplog_offset = self.get_op_logs(executor_log_path, oplog_offset)
            latslogs, latslog_offset = self.get_lats_logs(executor_log_path, latslog_offset)
            syslogs, sys_log_offset = self.get_sys_logs(executor_log_path, sys_log_offset)
            
            payload = {
                'executor_name': self.executor_name,
                'executor_uuid': self.executor_uuid,
                'worker_logs': json.dumps(wlogs),
                'output_processor_logs': json.dumps(oplogs),
                'lats_logs': json.dumps(latslogs),
                'syslogs': json.dumps(syslogs),
            }
            
            r = requests.post(url, data=json.dumps(payload))
            res = json.loads(r.text)
            r.close()

            if int(res["status"]) == 200:
                print(f"[{self.get_datetime()}] [log thread]\tSent logs to backend server.")
            else:
                print(f"[{self.get_datetime()}] [log thread]\tFailed to send logs to backend server.")

            sleep(timeout)

        # send final logs before stopping
        wlogs, wlog_offset = self.get_worker_logs(executor_log_path, wlog_offset)
        oplogs, oplog_offset = self.get_op_logs(executor_log_path, oplog_offset)
        latslogs, latslog_offset = self.get_lats_logs(executor_log_path, latslog_offset)
        syslogs, sys_log_offset = self.get_sys_logs(executor_log_path, sys_log_offset)
        
        payload = {
            'executor_name': self.executor_name,
            'executor_uuid': self.executor_uuid,
            'worker_logs': json.dumps(wlogs),
            'output_processor_logs': json.dumps(oplogs),
            'lats_logs': json.dumps(latslogs),
            'syslogs': json.dumps(syslogs),
        }

        r = requests.post(url, data=json.dumps(payload))
        res = json.loads(r.text)
        r.close()

        if int(res["status"]) == 200:
            print(f"[{self.get_datetime()}] [log thread]\tSent logs to backend server.")
        else:
            print(f"[{self.get_datetime()}] [log thread]\tFailed to send logs to backend server.")
        
        print(f"[{self.get_datetime()}] [log thread]\tStopped.")
        return

    def get_datetime(self) -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    def prefetch_fn(self, rank):
        num_submissions = -1
        request_url = f"http://{self.fetch_ip}:{self.fetch_port}/{self.fetch_route}"
        r = requests.get(request_url, params={"prefetch_factor": self.prefetch_factor})
        r.close()
        if r.status_code != 200:
            print(f"Message : {json.loads(r.text)}")
            print(f"Status Code : {r.status_code}")
        else:
            res = json.loads(r.text)
            if int(res["status"]) == 200:
                num_submissions = res["num_submissions"]
                if num_submissions != 0: 
                    submission_data = json.loads(res["jobs"])
                    queue_url = f"http://localhost:10001/submit-job"
                    r_ = requests.post(queue_url, data = json.dumps(submission_data))
                    r_.close()
                    print(f"[{self.get_datetime()}] [prefet_{rank}]\tQueued {num_submissions} Submissions in Job Queue | Current Queue Length : {len(redis_queue)}")
                else:
                    print(f"[{self.get_datetime()}] [prefet_{rank}]\tNo more submissions to fetch | Current Queue Length : {len(redis_queue)}")
                
                if num_submissions < self.threshold:
                    self.thread_res.set()
                else:
                    self.thread_res.clear()
            else:
                if self.register_executor():
                    self.prefetch_fn(rank)
                else:
                    print(f"[{self.get_datetime()}] [prefet_{rank}]\tFailed to register this executor| Current Queue Length : {len(redis_queue)}")

    def global_queue_fn(self):
        joined = 0
        spawned = 0
        timeout = 0.15
        queue_thread_timeout = 2
        queue_trottled = 0

        initial_prefetch_threads = self.num_prefetch_threads

        while not self.global_queue_thread.stopped():
            queue_length = len(redis_queue)

            if queue_length < self.threshold:

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
                        
                        if queue_thread_timeout > self.timeout:
                            queue_thread_timeout = self.timeout

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
    
    def global_execute(self, worker_target_fn, output_processor_target_fn) -> None:
        
        if not worker_target_fn:
            print("A target function is required for starting worker processes.")
            return

        if not output_processor_target_fn:
            print("A target function is required for starting output processor.")
            return

        if not self.worker_args:
            print("Worker Arguments not provided. Please register worker args before starting executor.")
            return

        if not self.output_processor_args:
            print("Output Processor Arguments not provided. Please register output processor args before starting executor.")
            return

        # spawn a queue thread in master process
        for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGCHLD]:
            signal.signal(signame, signal.SIG_DFL)

        self.global_queue_thread = QueueThread(target=self.global_queue_fn)
        self.global_queue_thread.start()

        sleep(5)
        # spawn multiple processes that read from the queue
        self.workers = self.spawn_workers(target_fn=worker_target_fn, args=self.worker_args)
        for workers in self.workers:
            workers.start()

        self.output_processor_proc = multiprocessing.Process(target=output_processor_target_fn, args=self.output_processor_args)
        self.output_processor_proc.start()

        self.log_thread = threading.Thread(target=self.logs_fn)
        self.log_thread.start()

        self.sys_thread = threading.Thread(target=self.sys_fn)
        self.sys_thread.start()

        for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGCHLD]:
            signal.signal(signame, self.signal_handler)
    
    def execute(self, worker_target_fn, output_processor_target_fn):
        self.global_execute(worker_target_fn=worker_target_fn, output_processor_target_fn=output_processor_target_fn)

    def register_worker_args(self, args=None):
        if not args:
            print("Worker arguments are required to start worker processes.")
            return

        self.worker_args = args

    def register_output_processor_args(self, args=None):
        if not args:
            print("Output Processor arguments are required to start output processor.")
            return

        self.output_processor_args = args

    def global_queue_cleanup(self) -> None:
        self.global_queue_thread.stop()

        for ix, thread in enumerate(self.prefetch_threads):
            if thread.is_alive():
                print(f"[{self.get_datetime()}] [master_p]\tForce Thread-{ix+1}.join()")
                thread.join()
    
    def global_cleanup(self) -> None:
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        print(f"[{self.get_datetime()}] [master_p]\tTerminating Queue Thread.")
        self.global_queue_cleanup()
        self.global_queue_thread.join(timeout=20)

        for ix, workers in enumerate(self.workers):
            print(f"[{self.get_datetime()}] [master_p]\tTerminating Worker {ix+1}.")
            if workers.is_alive():
                workers.join()
        
        print(f"[{self.get_datetime()}] [master_p]\tTerminating Output Processor.")
        if self.output_processor_proc.is_alive():
            self.output_processor_proc.join()

        print(f"[{self.get_datetime()}] [master_p]\tTerminating Sys Thread.")
        self.sys_thread.join(timeout=self.sys_timeout)
        print(f"[{self.get_datetime()}] [master_p]\tTerminating Log Thread.")
        print(f"[{self.get_datetime()}] [master_p]\tPlease wait until log thread sends latest logs to backend.")
        self.log_thread.join(timeout=self.log_timeout)
        broker.close()

    def cleanup(self):
        self.global_cleanup()

if __name__ == "__main__":

    from .worker import worker_fn
    from output_processor.output import output_processor_fn

    config_path = os.path.join(os.getcwd(),"config", "evaluator.json")

    configs = None
    with open(config_path, "r") as f:
        configs = json.loads(f.read())

    executor_config = configs["executor"]
    docker_config = configs["docker"]

    docker_ip = docker_config["docker_ip"]
    docker_port = docker_config["docker_port"]
    docker_route = docker_config["docker_route"]
    docker_image = docker_config["docker_image"]

    backend_cpu_limit: int = docker_config["cpu_limit"]
    backend_mem_limit: str = docker_config["memory_limit"]
    backend_cpu_taskset: bool = docker_config["taskset"]
    backend_host_output_dir: str = docker_config["shared_output_dir"]
    backend_docker_output_dir: str = docker_config["docker_output_dir"]
    backend_memswapiness: int = docker_config["docker_memswapiness"]
    backend_spawn_wait: int = docker_config["spawn_wait"]
    backend_blacklist_threshold: int = docker_config["blacklist_threshold"]
    backend_blacklist_duration: int = docker_config["blacklist_duration"]

    executor = ExecutorContext(
        name=executor_config["name"],
        fetch_ip=BACKEND_INTERNAL_IP,
        fetch_port=executor_config["fetch_port"],
        fetch_route=executor_config["fetch_route"],
        num_workers=executor_config["num_workers"],
        global_queue_thread=executor_config["global_queue_thread"],
        global_prefetch_thread=executor_config["global_prefetch_thread"],
        prefetch_threads=executor_config["prefetch_threads"],
        prefetch_factor=executor_config["prefetch_factor"],
        threshold=executor_config["threshold"],
        num_backends=executor_config["num_backends"],
        timeout=executor_config["timeout"],
        log_dir=executor_config["log_dir"],
        log_timeout=executor_config["log_timeout"],
        sys_timeout=executor_config["sys_timeout"],
        output_processor_threads = executor_config["output_processor_threads"],
        output_processor_timeout = executor_config["output_processor_timeout"],
        store_lats=executor_config["store_lats"],
        lats_dir=executor_config["lats_dir"],
    )

    print(executor)

    executor.register_worker_args(
        args=(
            executor.log_path,
            executor.team_dict, 
            executor.running_dict, 
            executor.lock, 
            executor.timeout, 
            docker_ip, 
            docker_port, 
            docker_route, 
            docker_image, 
            executor.num_threads,
            backend_cpu_limit, 
            backend_mem_limit,
            backend_cpu_taskset,
            backend_memswapiness, 
            backend_host_output_dir, 
            backend_docker_output_dir, 
            backend_spawn_wait,
            backend_blacklist_threshold,
            backend_blacklist_duration
        )
    )

    executor.register_output_processor_args(
        args=(
            1, # rank
            executor.log_path,
            executor.output_processor_event, 
            executor.output_processor_threads,
            executor.output_processor_timeout,
            docker_config["shared_output_dir"],
            os.path.join(os.getcwd(), "answer"),
            executor.store_lats,
            executor.lats_path
        )
    )

    executor.execute(
        worker_target_fn = worker_fn,
        output_processor_target_fn = output_processor_fn
    )

    signal.pause()