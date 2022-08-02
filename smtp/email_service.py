import os
import time
import smtplib
from smtp import dbuser

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class EmailingService:
    @staticmethod
    def get_connection(user, password) -> smtplib.SMTP_SSL:
        '''
        This function returns the SMTP server connection.

        Parameters:
        -----------
        None

        Returns:
        --------
        None
        '''

        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(user, password)
        return server

    @staticmethod
    def get_email_list(teamId: int) -> list:
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

        team = dbuser.find_one({'teamID': str(teamId)})
        return team['emails']

    @staticmethod
    def get_subject(submissionId: int) -> str:
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

    @staticmethod
    def get_body(submissionId: int, submissionStatus: str) -> MIMEText:
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

    # @staticmethod
    # def send_email(teamId: int, submissionId: int, submissionStatus: str) -> None:
    #     '''
    #     This function sends an email to the team members of the team.

    #     Parameters:
    #     -----------
    #     teamId: int
    #         The team ID of the team to send the email to.
    #     submissionId: int
    #         The submission ID of the submission to send the email about.
    #     submissionStatus: str
    #         The status of the submission to send the email about.

    #     Returns:
    #     --------
    #     None
    #     '''

        # Get emails from team ID
        # emails = get_email_list(teamId)
        # if emails is None:
        #     return
        
        # # Get to-addresses, subject and body
        # subject = self.get_subject(submissionId)
        # html = self.get_body(submissionId, submissionStatus)
        # msg = MIMEMultipart('alternative')
        # msg['Subject'] = subject
        # msg['From'] = self.sent_from
        # msg['To'] = ', '.join(emails)
        # msg.attach(html)

        # # Send the email
        # try:
        #     self.server.sendmail(self.sent_from, emails, msg.as_string())
        # except Exception as e:
        #     retry_count = 0
        #     while retry_count < 3:
        #         self.get_connection()
        #         try:
        #             self.server.sendmail(self.sent_from, emails, msg.as_string())
        #             break
        #         except Exception as e:
        #             retry_count += 1
        #             time.sleep(2)
        #             continue
        #     if retry_count == 3:
        #         print(f'Failed to send email to {emails}')
        #         print(e)
        #         return
