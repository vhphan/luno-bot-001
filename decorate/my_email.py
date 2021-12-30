# import the smtplib module. It should be included in Python by default
import smtplib
# set up the SMTP server
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import dotenv_values

config = dotenv_values('/home2/eproject/dnb/.env')
MAIL_ADDRESS = config['email_user']
MAIL_PASSWORD = config['email_password']
DEFAULT_EMAIL_RECIPIENT = config['default_email_recipient']


def send_eri_mail(message_, recipient=DEFAULT_EMAIL_RECIPIENT, subject="This is TEST", message_type='html', attachments=None):
    msg = MIMEMultipart()  # create a message
    # setup the parameters of the message
    msg['From'] = MAIL_ADDRESS
    msg['To'] = recipient
    msg['Subject'] = subject
    # add in the message body
    msg.attach(MIMEText(message_, message_type))
    # send
    with smtplib.SMTP_SSL(host='mail.eprojecttrackers.com', port=465) as s:
        s.login(MAIL_ADDRESS, MAIL_PASSWORD)
        s.send_message(msg, MAIL_ADDRESS, recipient)


if __name__ == '__main__':
    message = """
        <table style="width:100%">
          <tr>
            <th>Firstname</th>
            <th>Lastname</th>
            <th>Age</th>
          </tr>
          <tr>
            <td>Jill</td>
            <td>Smith</td>
            <td>50</td>
          </tr>
          <tr>
            <td>Eve</td>
            <td>Jackson</td>
            <td>94</td>
          </tr>
        </table>
        """

    send_eri_mail(DEFAULT_EMAIL_RECIPIENT, message)
