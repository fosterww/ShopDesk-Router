from pathlib import Path
from common.ingest.email_parser import parse_email
import pytest

def test_parse_email_ignores_inline_images():
    raw = Path("tests/data/sample_multipart.eml").read_bytes()
    body_text, atts = parse_email(raw)
    assert body_text
    mimes = {a["mime"] for a in atts}
    names = {a["filename"] for a in atts}
    assert "application/pdf" in mimes
    assert any(n.endswith(".pdf") for n in names)
    assert any(m in mimes for m in ("audio/ogg", "audio/mpeg", "audio/mp4"))
    assert not any(m.startswith("image/") for m in mimes)

