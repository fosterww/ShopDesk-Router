from __future__ import annotations
from email import policy
from email.parser import BytesParser
from email.message import EmailMessage
from typing import Tuple, List, Dict, Any
import re


_HTML_TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(html: str) -> str:
    return _HTML_TAG_RE.sub(" ", html).replace("&nbsp;", " ").strip()


def extract_best_text(msg: EmailMessage) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/plain" and "attachments" not in disp:
                content = part.get_content()
                return content.strip() if isinstance(content, str) else str(content).strip()
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/html" and "attachment" not in disp:
                content = part.get_content()
                return html_to_text(content if isinstance(content, str) else str(content))
        return ""
    else:
        ctype = msg.get_content_type()
        if ctype == "text/plain":
            content = msg.get_content()
            return content.strip() if isinstance(content, str) else str(content).strip()
        if ctype == "text/html":
            content = msg.get_content()
            return html_to_text(content if isinstance(content, str) else str(content))
        return ""


def parse_email(raw_bytes: bytes) -> Tuple[str, List[Dict[str, Any]]]:
    msg: EmailMessage = BytesParser(policy=policy.default).parsebytes(raw_bytes)
    body_text = extract_best_text(msg)
    html_text: str | None = None
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_text = part.get_content()
                break

    atts: List[Dict[str, Any]] = []
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
            atts.append(
                {
                    "filename": filename or f"part-{id(part)}.{ctype.split('/')[-1]}",
                    "mime": ctype,
                    "bytes": (
                        payload
                        if isinstance(payload, (bytes, bytearray))
                        else payload.encode("utf-8", errors="ignore")
                    ),
                }
            )

        elif ctype in ("application/pdf", "audio/ogg", "audio/mpeg", "audio/mp4"):
            payload = part.get_content()
            atts.append(
                {
                    "filename": filename or f"part-{id(part)}.{ctype.split('/')[-1]}",
                    "mime": ctype,
                    "bytes": (
                        payload
                        if isinstance(payload, (bytes, bytearray))
                        else payload.encode("utf-8", errors="ignore")
                    ),
                }
            )

    return body_text, atts
