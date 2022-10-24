class MRJob:
    team_id: str = None
    assignment_id: str = None
    timeout: int = 120
    submission_id: str = "submissionId1"
    mapper: bytes = None
    reducer: bytes = None

    def __init__(self, **kwargs) -> None:
        for k,v in kwargs.items():
            setattr(self, k, v)

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

class SparkJob:
    team_id: str = None
    assignment_id: str = None
    timeout: int = 120
    submission_id: str = "submissionId1"
    spark: bytes = None

    def __init__(self, **kwargs) -> None:
        for k,v in kwargs.items():
            setattr(self, k, v)

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
    team_id: str = None
    assignment_id: str = None
    timeout: int = 120
    submission_id: str = "submissionId1"
    producer: bytes = None
    consumer: bytes = None

    def __init__(self, **kwargs) -> None:
        for k,v in kwargs.items():
            setattr(self, k, v)

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