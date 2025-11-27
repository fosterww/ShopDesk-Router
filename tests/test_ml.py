import pytest

pytest.importorskip("transformers")

from common.ml.docqa import extract_fields, extract_fields_sync
from common.ml.asr import transcribe, transcribe_sync


@pytest.mark.anyio("asyncio")
async def test_docqa_async_stub():
    fields = await extract_fields(b"dummy", "application/pdf")
    assert fields.order_id == "A10023"


def test_docqa_sync_stub():
    fields = extract_fields_sync(b"dummy", "application/pdf")
    assert fields.order_id == "A10023"


@pytest.mark.anyio("asyncio")
async def test_asr_async_stub(monkeypatch):
    monkeypatch.setattr("common.ml.asr.use_stub", lambda: True)
    t = await transcribe(b"audio-bytes", "audio/ogg")
    assert "refund" in t.text.lower()
    assert t.confidence == pytest.approx(0.97)


def test_asr_sync_stub(monkeypatch):
    monkeypatch.setattr("common.ml.asr.use_stub", lambda: True)
    t = transcribe_sync(b"audio-bytes", "audio/ogg")
    assert "refund" in t.text.lower()
    assert t.confidence == pytest.approx(0.97)
