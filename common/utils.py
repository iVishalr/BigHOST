from datetime import datetime

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    def flush(self):
        pass

def get_datetime() -> str:
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    return timestamp