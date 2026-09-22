import os
import base64
import mimetypes
from email.message import EmailMessage

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
TOKEN_PATH = "token.json"
CREDENTIALS_PATH = "credentials.json"
# Must be registered as an "Authorized redirect URI" on the OAuth client in
# Google Cloud Console (Web application type, not Desktop). Override in .env
# if the server runs somewhere other than localhost:8000.
REDIRECT_URI = os.environ.get(
    "GMAIL_OAUTH_REDIRECT_URI", "http://localhost:8000/api/gmail/oauth2callback"
)


class GmailAuthRequired(Exception):
    """Raised instead of blocking when there's no valid token. Send the dev
    to `auth_url`; once they approve, exchange_code() finishes the flow."""

    def __init__(self, auth_url: str):
        self.auth_url = auth_url
        super().__init__(f"Gmail needs re-authorization: {auth_url}")


class GmailService:
    """Does NOT authenticate on construction — no more blocking on
    run_local_server() (which used to hang the FastAPI process waiting for a
    local browser, and is why you had to run it on your own machine and copy
    token.json in by hand).

    - If token.json is valid, or expired-but-refreshable: works silently,
      exactly like before.
    - If there's no usable token (first run, or the refresh token itself was
      revoked/expired): raises GmailAuthRequired with a URL. Send the dev
      there once; /api/gmail/oauth2callback exchanges the code and writes a
      fresh token.json. No more manual copy-paste between environments.
    """

    def __init__(self):
        self._service = None

    # -- token handling -----------------------------------------------------

    def _load_creds(self):
        if not os.path.exists(TOKEN_PATH):
            return None
        try:
            return Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception:
            return None  # corrupt/incompatible token.json -> treat as absent

    def _save_creds(self, creds):
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())

    def get_service(self):
        if self._service is not None:
            return self._service

        creds = self._load_creds()

        if creds and creds.valid:
            self._service = build("gmail", "v1", credentials=creds)
            return self._service

        if creds and creds.expired and creds.refresh_token:
            # This is the normal "token expired" case — silent, no popup.
            creds.refresh(Request())
            self._save_creds(creds)
            self._service = build("gmail", "v1", credentials=creds)
            return self._service

        # No token, or refresh_token itself is dead — needs a human to
        # re-consent once. Don't block; hand the URL back to the caller.
        raise GmailAuthRequired(self.get_auth_url())

    def is_authenticated(self) -> bool:
        creds = self._load_creds()
        return bool(creds and (creds.valid or (creds.expired and creds.refresh_token)))

    # -- OAuth flow (web-redirect, not run_local_server) --------------------

    def get_auth_url(self) -> str:
        flow = Flow.from_client_secrets_file(
            CREDENTIALS_PATH, scopes=SCOPES, redirect_uri=REDIRECT_URI
        )
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",  # forces Google to re-issue a refresh_token
        )
        return auth_url

    def exchange_code(self, code: str):
        """Called by the /oauth2callback route after the dev approves."""
        flow = Flow.from_client_secrets_file(
            CREDENTIALS_PATH, scopes=SCOPES, redirect_uri=REDIRECT_URI
        )
        flow.fetch_token(code=code)
        self._save_creds(flow.credentials)
        self._service = build("gmail", "v1", credentials=flow.credentials)

    # -- sending (unchanged behavior) ---------------------------------------

    def send_mail(self, to_email, subject, body, attachment_path=None,
                  attachment_bytes=None, attachment_filename=None):
        service = self.get_service()  # may raise GmailAuthRequired

        message = EmailMessage()
        message["To"] = to_email
        message["Subject"] = subject
        message.set_content(body)

        if attachment_bytes is not None and attachment_filename:
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
        send_message = service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        print("✅ Email sent successfully!")
        print("Message ID:", send_message["id"])
        return send_message["id"]
