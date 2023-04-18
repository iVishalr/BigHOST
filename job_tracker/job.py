import time

class Job:
    """
    Base class for each Job Type
    """
    def __init__(self, team_id=None, assignment_id=None, timeout=None, submission_id=None) -> None:
        
        self.team_id: str = team_id
        self.assignment_id: str = assignment_id
        self.submission_id: str = str(submission_id)
        self.timeout: int = timeout

        self.timestamps = {
            "received": 0,
            "sanity_check_start": 0,
            "sanity_check_end": 0,
            "waiting_queue_entry": 0,
            "waiting_queue_exit": 0,
            "processing_queue_entry": 0,
            "processing_queue_exit": 0,
            "processing_start": 0,
            "processing_end": 0,
            "output_queue_entry": 0,
            "output_queue_exit": 0,
            "output_processing_start": 0,
            "output_processing_end": 0,
            "completed": 0,
        }

        self.blacklisted = False
        self.end_time = None
        self.status = ""
        self.message = ""


    def record(self, key=None):
        if key not in self.timestamps.keys():
            print(f"Invalid Key {key}.")
            return
        
        if self.timestamps[key] != 0:
            print(f"Timestamp for {key} was already recorded.")
            return

        self.timestamps[key] = time.time_ns()
    
    def reset(self):
        for k in self.timestamps:
            self.timestamps[k] = 0

    def get_timestamp_keys(self):
        return list(self.timestamps.keys())

    def get_waiting_time(self):
        wq_time = (self.timestamps["waiting_queue_exit"] - self.timestamps["waiting_queue_entry"]) / 1e9
        procq_time = (self.timestamps["processing_queue_exit"] - self.timestamps["processing_queue_entry"]) / 1e9
        op_time = (self.timestamps["output_queue_exit"] - self.timestamps["output_queue_entry"]) / 1e9
        total_wait_time = wq_time + op_time + procq_time
        waiting_time = {
            "waiting_queue": wq_time,
            "processing_queue": procq_time,
            "output_queue": op_time,
            "total": total_wait_time
        }
        return waiting_time

    def get_processing_time(self):
        proc_time = (self.timestamps["processing_end"] - self.timestamps["processing_start"]) / 1e9
        op_proc_time = (self.timestamps["output_processing_end"] - self.timestamps["output_processing_start"]) / 1e9
        sc_proc_time = (self.timestamps["sanity_check_end"] - self.timestamps["sanity_check_start"]) / 1e9
        total_processing_time = proc_time + op_proc_time + sc_proc_time
        processing_time = {
            "processing_time": proc_time,
            "output_processing_time": op_proc_time,
            "sanity_check_time": sc_proc_time,
            "total": total_processing_time,
        }
        return processing_time

    def get_end2end_latency(self):
        e2e = (self.timestamps["completed"] - self.timestamps["received"]) / 1e9
        return {'end2end': e2e}

    def get_total_time(self):
        processing_time = self.get_processing_time()
        waiting_time = self.get_waiting_time()
        end2end_time = self.get_end2end_latency()
        
        individual_time = {}
        for d in [processing_time, waiting_time, end2end_time]:
            for k,v in d.items():
                if k == "total":
                    continue
                individual_time[k] = v
        
        total_time = {
            "summary": {
                "processing_time": processing_time["total"],
                "waiting_time": waiting_time["total"],
                "total_time": end2end_time["end2end"]
            },
            "individual": individual_time,
            "total_time": end2end_time["end2end"],
            "received": self.timestamps["received"],
            "completed": self.timestamps["completed"]
        }

        return total_time

    def get_db_fields(self):
        data = {
            "teamId": self.team_id,
            "assignmentId": self.assignment_id,
            "submissionId": self.submission_id,
        }
        return data

class MRJob(Job):
    """
    Job class for MapReduce Jobs
    """
    def __init__(self, team_id, assignment_id, submission_id, timeout=None, mapper=None, reducer=None) -> None:
        
        self.mapper: bytes = mapper
        self.reducer: bytes = reducer
        
        super().__init__(
            team_id=team_id, 
            assignment_id=assignment_id, 
            submission_id=submission_id,
            timeout=timeout
        )

    def __str__(self) -> str:
        buffer = []
        buffer.append(f"Job Object")
        buffer.append(f"Team ID : {self.team_id}")
        buffer.append(f"Assignment ID : {self.assignment_id}")
        buffer.append(f"Timeout : {self.timeout}")
        buffer.append(f"Submission ID : {self.submission_id}")
        buffer.append(f"Mapper : {self.mapper}\n")
        buffer.append(f"Reducer : {self.reducer}\n")
        return "\n".join(buffer)

class SparkJob(Job):
    """
    Job class for Spark Jobs
    """
    def __init__(self, team_id, assignment_id, submission_id, timeout=None, spark=None) -> None:
        self.spark: bytes = spark

        super().__init__(
            team_id=team_id,
            assignment_id=assignment_id,
            submission_id=submission_id,
            timeout=timeout
        )

    def __str__(self) -> str:
        buffer = []
        buffer.append(f"Job Object")
        buffer.append(f"Team ID : {self.team_id}")
        buffer.append(f"Assignment ID : {self.assignment_id}")
        buffer.append(f"Timeout : {self.timeout}")
        buffer.append(f"Submission ID : {self.submission_id}")
        buffer.append(f"Spark : {self.spark}\n")
        return "\n".join(buffer)

class KafkaJob:
    """
    Job class for Kafka Jobs
    """
    def __init__(self, team_id, assignment_id, submission_id, timeout=None, producer=None, consumer=None) -> None:
        self.producer = producer
        self.consumer = consumer
        super().__init__(
            team_id=team_id,
            assignment_id=assignment_id,
            submission_id=submission_id,
            timeout=timeout
        )

    def __str__(self) -> str:
        buffer = []
        buffer.append(f"Job Object")
        buffer.append(f"Team ID : {self.team_id}")
        buffer.append(f"Assignment ID : {self.assignment_id}")
        buffer.append(f"Timeout : {self.timeout}")
        buffer.append(f"Submission ID : {self.submission_id}")
        buffer.append(f"Producer : {self.producer}\n")
        buffer.append(f"Consumer : {self.consumer}\n")
        return "\n".join(buffer)

def job_template_selector(assignment_id):
    if "A1" in assignment_id or "A2" in assignment_id:
        return MRJob
    elif "A3" in assignment_id:
        if "T1" in assignment_id:
            return SparkJob
        else:
            return KafkaJob