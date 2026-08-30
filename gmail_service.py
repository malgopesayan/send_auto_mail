import os
import base64
import mimetypes
from email.message import EmailMessage

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


class GmailService:
    def __init__(self):
        self.service = self.authenticate()

    def authenticate(self):
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        return build("gmail", "v1", credentials=creds)

    def send_mail(self, to_email, subject, body, attachment_path=None,
                  attachment_bytes=None, attachment_filename=None):
        message = EmailMessage()
        message["To"] = to_email
        message["Subject"] = subject
        message.set_content(body)

        if attachment_bytes is not None and attachment_filename:
            # Attach in-memory bytes (e.g. a resume downloaded from Supabase Storage)
            mime_type, _ = mimetypes.guess_type(attachment_filename)
            if mime_type is None:
                mime_type = "application/octet-stream"
            main_type, sub_type = mime_type.split("/", 1)
            message.add_attachment(attachment_bytes, maintype=main_type, subtype=sub_type,
                                    filename=attachment_filename)
        elif attachment_path:
            mime_type, _ = mimetypes.guess_type(attachment_path)
            if mime_type is None:
                mime_type = "application/octet-stream"
            main_type, sub_type = mime_type.split("/", 1)
            with open(attachment_path, "rb") as f:
                message.add_attachment(f.read(), maintype=main_type, subtype=sub_type,
                                        filename=os.path.basename(attachment_path))
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        send_message = self.service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        print("✅ Email sent successfully!")
        print("Message ID:", send_message["id"])
