from __future__ import annotations
import base64
from typing import Dict
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


class GmailClient:
    def __init__(self, creds: Credentials, user_id: str = "me"):
        self.user_id = user_id
        self.svc = build("gmail", "v1", credentials=creds, cache_discovery=False)

    def list_message_ids(self, query: str, max_results: int = 25) -> list[str]:
        resp = self.svc.users().messages().list(
            userId=self.user_id, q=query, maxResults=max_results
        ).execute()
        return [m["id"] for m in resp.get("messages", [])]
    
    def get_raw_message(self, msg_id: str) -> bytes:
        resp = self.svc.users().messages().get(
            userId=self.user_id, id=msg_id, format="raw"
        ).execute()
        return base64.urlsafe_b64decode(resp["raw"])
    
    def get_headers(self, msg_id: str) -> Dict[str, str]:
        resp = self.svc.users().messages().get(
             userId=self.user_id, id=msg_id, format="metadata", metadataHeaders=[
                 "Message-Id","Subject","From","Date"
             ]
        ).execute()
        headers = {h["name"].lower(): h["value"] for h in resp["payload"]["headers"]}
        return {
            "message_id": headers.get("message-id", ""),
            "subject": headers.get("subject", ""),
            "from": headers.get("from", ""),
            "date": headers.get("date", ""),
        }
    