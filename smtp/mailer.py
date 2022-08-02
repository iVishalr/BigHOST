import os
import sys
import json
import pickle
import signal
import filecmp
import requests
import threading
from time import sleep
from typing import List
from datetime import datetime
from smtp import mail_broker, mail_queue, mail_user, mail_server
from smtp.email_service import EmailingService

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def output_processor_fn():
    '''
    Takes a messages from mail-queue and send mails
    '''
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

    print(f"[{get_datetime()}] [Mailer]\tStarting Mailer.")

    f = open(f'./mailer_logs.txt', 'w+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    interval = 0.05
    timeout = 0.05
    process_slept = 0

    event = threading.Event()

    mailer = EmailingService()

    while not event.is_set():

        if len(mail_queue)==0:
            timeout += 0.05
            interval += timeout
            
            if interval > 30:
                interval = 30

            process_slept = 1
            print(f"[{get_datetime()}] [mailer]\tSleeping Mailer for {interval:.04f} seconds.")
            sleep(interval)
            continue

        else:
            interval = 0.05
            timeout = 0.05
            if process_slept:
                print(f"[{get_datetime()}] [mailer]\tWaking up Mailer.")
                process_slept = 0

        mail_message = mail_queue.dequeue()
        
        if mail_message is None:
            process_slept = 1
            print(f"[{get_datetime()}] [mailer]\tSleeping Mailer for {interval:.04f} seconds.")
            sleep(interval)
            continue

        queue_name, mail_message = mail_message
        mail_message = pickle.loads(mail_message)
        
        teamId = mail_message["teamId"]
        submissionId = int(mail_message["submissionId"])
        submissionStatus = str(mail_message["submissionStatus"])

        emails = mailer.get_email_list(teamId)
        if emails is None:
            continue

        subject = mailer.get_subject(submissionId)
        html = mailer.get_body(submissionId, submissionStatus)
        sent_from = f'Big Data PES University <{mail_user}>'
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sent_from
        msg['To'] = ', '.join(emails)
        msg.attach(html)

        mail_server.sendmail(sent_from, emails, msg.as_string())

    def signal_handler(sig, frame):
        print(f'[{get_datetime()}] [output_processor]\tStopping.')
        event.set()
        mail_broker.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)    

if __name__ == "__main__":
    output_processor_fn()
    signal.pause()