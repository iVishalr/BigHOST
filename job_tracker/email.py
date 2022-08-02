import os
import time
import smtplib
from dotenv import load_dotenv
from pymongo import MongoClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailingService:
    def __init__(self):
        load_dotenv()
        # Set up user, password and a sent-from email address
        self.user = 'bigadata@pes.edu'
        self.password = 'bigdata_2022'
        self.sent_from = f'Big Data PES University <{self.user}>'

        # Fire up the SMTP server and login
        self.server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        self.server.ehlo()
        self.server.login(self.user, self.password)

        # Connect to MongoDB
        client = MongoClient(os.getenv('MONGO_URI'), connect=False)
        db = client['bd']
        self.dbuser = db['user']
    

    def get_connection(self) -> None:
        '''
        This function returns the SMTP server connection.

        Parameters:
        -----------
        None

        Returns:
        --------
        None
        '''

        self.server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        self.server.ehlo()
        self.server.login(self.user, self.password)


    def get_email_list(self, teamId: int) -> list:
        '''
        This function returns the list of emails of the team members of the team.

        Parameters:
        -----------
        teamID: int
            The team ID of the team to get the list of emails of.

        Returns:
        --------
        emails: list
            The list of emails of the team members of the team.
        '''

        team = self.dbuser.find_one({'teamID': str(teamId)})
        return team['emails']


    def get_subject(self, submissionId: int) -> str:
        '''
        This function returns the subject of the email to be sent.

        Parameters:
        -----------
        submissionId: int
            The submission ID of the submission to send the email about.

        Returns:
        --------
        subject: str
            The subject of the email to be sent.
        '''

        return f'Update on submission ID {submissionId}'


    def get_body(self, submissionId: int, submissionStatus: str) -> MIMEText:
        '''
        This function returns the body of the email to be sent.

        Parameters:
        -----------
        submissionId: int
            The submission ID of the submission to send the email about.
        submissionStatus: str
            The status of the submission to send the email about.

        Returns:
        --------
        html: MIMEText
            The html body of the email to be sent as a MIMEText object.
        '''

        html = f"""\
        <html>
        <head></head>
        <body>
            <p>Hello,</p>
            <p>This is an auto-update on the status of your submission: <strong>{submissionId}</strong></p>
            <p>The current status is: <strong>{submissionStatus}</strong></p>
            <p>Thank you,</p>
            <p>Big Data PES University</p>
            <p>(This is an automated message. Please do not reply.)</p>
        </body>
        </html>
        """
        part = MIMEText(html, 'html')
        return part


    def send_email(self, teamId: int, submissionId: int, submissionStatus: str) -> None:
        '''
        This function sends an email to the team members of the team.

        Parameters:
        -----------
        teamId: int
            The team ID of the team to send the email to.
        submissionId: int
            The submission ID of the submission to send the email about.
        submissionStatus: str
            The status of the submission to send the email about.

        Returns:
        --------
        None
        '''

        # Get emails from team ID
        emails = self.get_email_list(teamId)
        if emails is None:
            return
        
        # Get to-addresses, subject and body
        subject = self.get_subject(submissionId)
        html = self.get_body(submissionId, submissionStatus)
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.sent_from
        msg['To'] = ', '.join(emails)
        msg.attach(html)

        # Send the email
        try:
            self.server.sendmail(self.sent_from, emails, msg.as_string())
        except Exception as e:
            retry_count = 0
            while retry_count < 3:
                self.get_connection()
                try:
                    self.server.sendmail(self.sent_from, emails, msg.as_string())
                    break
                except Exception as e:
                    retry_count += 1
                    time.sleep(2)
                    continue
            if retry_count == 3:
                print(f'Failed to send email to {emails}')
                print(e)
                return
