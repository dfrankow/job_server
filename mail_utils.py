import os.path
import smtplib
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import Encoders


# All emails must match this regular expression if it is set
mail_filter = None


def attachment_from_file(filepath):
    """Create a mime attachment from a file."""
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(open(filepath, 'rb').read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment; filename="%s"' % os.path.basename(filepath))
    return part


def send_simple_mail(smtp_host, from_address, to_address, subject,
                     text_content, attachments=None, bcc_address=None):
    """
    Send an email using SMTP.

    If mail_filter set, all email addresses must match that regular expression.

    smtp_host: host of the SMTP server
    to_address: The email address to send to.
    from_address: The email address the email is coming from.
    subject: The subject line.
    text_content: The plaintext body of the email.
    bcc_address: An optional blind-carpon-copy string (extra addresses)
      to use when sending the email.
    attachments: a list of attachments to attach to the email. None by default.
      Caller must ensure the attachments can attach to MIMEMultipart
      and have all of the relevant headers for their use.

    Example:
      send_simple_mail('localhost', 'dan@example.com',
        ['dan@example.com'], "hi dan", "this is some text")
    """
    recipients = [recipient.strip() for recipient in to_address.split(',')]
    if bcc_address:
        bcc = [recipient.strip() for recipient in bcc_address.split(',')]
        recipients += recipients + bcc

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = ', '.join(recipients)

    if mail_filter and any(not re.search(mail_filter, recipient)
                           for recipient in recipients):
        raise Exception("Recipient email addresses must match %s"
                        % mail_filter)

    text_part = MIMEText(text_content, 'plain')

    for attachment in (attachments or []):
        msg.attach(attachment)
    msg.attach(text_part)

    smtp = smtplib.SMTP(smtp_host)
    smtp.sendmail(from_address, recipients, msg.as_string())
    smtp.quit()
