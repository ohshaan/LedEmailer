import logging
import os
from email.message import EmailMessage
import smtplib

class EmailSendError(Exception):
    """Raised if sending email fails."""

def send_email_with_excel(
    recipient: str,
    file_path: str,
    metadata: dict,
    requested_ledgers: list,
    vault_url: str = None,       # For compatibility, not used here
    credential = None,           # For compatibility, not used here
    smtp_server: str = None,
    smtp_port: int = None,
    smtp_username: str = None,
    smtp_password: str = None,
    subject: str = "Ledger Report",
    body: str = None,
    cleanup: bool = False,       # If True, delete the file after sending
    retries: int = 1             # How many times to retry sending on failure
):
    """
    Sends the Excel report as an attachment to the given recipient.
    SMTP credentials must be provided.
    If cleanup=True, the file will be deleted after sending (even on error).
    """
    logging.info("Preparing to send email with Excel attachment.")
    if not smtp_server or not smtp_port or not smtp_username or not smtp_password:
        raise ValueError("SMTP credentials (server, port, username, password) are required")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = smtp_username
    msg['To'] = recipient if isinstance(recipient, str) else ", ".join(recipient)

    # Create the default body if not provided
    if not body:
        lines = "\n".join(f"- {metadata[l]['code']} – {metadata[l]['name']}" for l in requested_ledgers if l in metadata)
        body = f"Requested Ledgers:\n{lines}\n\nPlease find attached the requested ledger report."
    msg.set_content(body)

    try:
        with open(file_path, 'rb') as f:
            msg.add_attachment(
                f.read(),
                maintype='application',
                subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                filename=os.path.basename(file_path)
            )
    except Exception as e:
        logging.error(f"Failed to read Excel attachment: {e}")
        if cleanup:
            try:
                os.remove(file_path)
            except Exception as cleanup_error:
                logging.warning(f"Failed to delete file after read error: {cleanup_error}")
        raise

    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Connecting to SMTP server {smtp_server}:{smtp_port} as {smtp_username} (attempt {attempt})")
            with smtplib.SMTP(smtp_server, smtp_port) as smtp:
                smtp.starttls()
                smtp.login(smtp_username, smtp_password)
                smtp.send_message(msg, from_addr=smtp_username, to_addrs=[recipient] if isinstance(recipient, str) else recipient)
            logging.info(f"Email sent successfully to {recipient}")
            break  # Success, exit retry loop
        except Exception as e:
            logging.error(f"Failed to send email on attempt {attempt}: {e}")
            last_exception = e
            if attempt == retries:
                if cleanup:
                    try:
                        os.remove(file_path)
                    except Exception as cleanup_error:
                        logging.warning(f"Failed to delete file after send error: {cleanup_error}")
                raise EmailSendError(f"Failed to send email after {retries} attempts: {last_exception}")

    if cleanup:
        try:
            os.remove(file_path)
            logging.info(f"Deleted attachment file: {file_path}")
        except Exception as e:
            logging.warning(f"Failed to delete attachment file: {e}")
