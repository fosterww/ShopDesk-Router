from __future__ import annotations
from email import policy
from email.parser import BytesParser
from email.message import Message
from typing import Tuple, List, Dict
import re


_HTML_TAG_RE = re.compile(r"<[^>]+>")

def html_to_text(html: str) -> str:
    return _HTML_TAG_RE.sub(" ", html).replace("&nbsp;", " ").strip()

def extract_best_text(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/plain" and "attachments" not in disp:
                return part.get_content().strip()
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/html" and "attachment" not in disp:
                return html_to_text(part.get_content())
        return ""
    else:
        ctype = msg.get_content_type()
        if ctype == "text/plain":
            return msg.get_content().strip()
        if ctype == "text/html":
            return html_to_text(msg.get_content())
        return ""
    

def parse_email(raw_bytes: bytes) -> Tuple[str, List[Dict]]:
    msg = BytesParser(policy=policy.default).parsebytes(raw_bytes)
    body_text = extract_best_text(msg)
    html_text = None
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_text = part.get_content()
                break

    atts: List[Dict] = []
    for part in msg.walk():
        ctype = part.get_content_type()
        disp = (part.get("Content-Disposition") or "").lower()
        filename = part.get_filename()
        if part.is_multipart():
            continue

        cid = (part.get("Content-ID") or "").strip("<>")
        if ctype.startswith("image/") and cid and html_text and f"cid:{cid}" in html_text:
            continue

        if "attachment" in disp or filename:
            payload = part.get_content()
            atts.append({
                "filename": filename or f"part-{id(part)}.{ctype.split('/')[-1]}",
                "mime": ctype,
                "bytes": payload if isinstance(payload, (bytes, bytearray))
                         else payload.encode("utf-8", errors="ignore"),
            })

        elif ctype in ("application/pdf", "audio/ogg", "audio/mpeg", "audio/mp4"):
            payload = part.get_content()
            atts.append({
                "filename": filename or f"part-{id(part)}.{ctype.split('/')[-1]}",
                "mime": ctype,
                "bytes": payload if isinstance(payload, (bytes, bytearray))
                         else payload.encode("utf-8", errors="ignore"),
            })

    return body_text, atts
