from redis import Redis

class RedisQueue:
    def __init__(self, broker: Redis, queue_name: str) -> None:
        self.broker = broker
        self.queue_name = queue_name

    def enqueue(self, data):
        self.broker.rpush(self.queue_name, data)
        return

    def dequeue(self):
        return self.broker.blpop(self.queue_name)

    def empty_queue(self):
        while self.__len__() > 0:
            _ = self.broker.blpop(self.queue_name)
    
    def is_empty(self) -> bool:
        return True if self.__len__() == 0 else False

    def __len__(self) -> int:
        return self.broker.llen(self.queue_name)