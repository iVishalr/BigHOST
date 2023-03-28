import sys
from datetime import datetime

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    def flush(self):
        pass

class Logger(object):
    def __init__(self, filename, mode):
        self.console = sys.stdout
        self.file = open(filename, mode)
 
    def write(self, message):
        self.console.write(message)
        self.file.write(message)
 
    def flush(self):
        self.console.flush()
        self.file.flush()

def get_datetime() -> str:
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    return timestamp