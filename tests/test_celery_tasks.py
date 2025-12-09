import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from worker.jobs import celery_tasks


def _make_session(first_value):
    class _Result:
        def __init__(self, val):
            self._val = val

        def first(self):
            return self._val

    class _Session:
        def __init__(self):
            self.commit = AsyncMock()

        async def execute(self, *_args, **_kwargs):
            return _Result(first_value)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    return _Session()


class _FakeRepo:
    def __init__(self):
        self.events = []
        self.get_last_event_result = None
        self.get_last_event_calls = []
        self.event_by_type: dict[str, object] | None = None

    async def get_last_event(self, *, message_id: str, type_: str):
        self.get_last_event_calls.append((message_id, type_))
        if self.event_by_type is not None and type_ in self.event_by_type:
            return self.event_by_type[type_]
        return self.get_last_event_result

    async def insert_event(self, *, ticket_id, message_id, type_, payload):
        self.events.append((ticket_id, message_id, type_, payload))


def test_celery_tasks_registered():
    try:
        from worker.celery_app import app
    except ImportError as exc:
        pytest.fail(f"Celery not available: {exc}. Install celery to run this test.")

    expected = {
        "ping",
        "pipeline.asr",
        "pipeline.docqa",
        "pipeline.zeroshot",
        "pipeline.summarize",
        "pipeline.vqa",
        "pipeline.normalized",
        "pipeline.ingested",
        "pipeline.docqa_select",
        "pipeline.create_ticket",
        "pipeline.run",
        "worker.jobs.gmail_poll.poll_gmail",
    }

    missing = expected.difference(app.tasks.keys())
    assert not missing, f"Tasks not registered: {missing}"


@pytest.mark.anyio
async def test_asr_task_missing_attachment(monkeypatch):
    session = _make_session(first_value=None)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)

    result = await celery_tasks._asr_task("att-1")

    assert result is None
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_asr_task_skips_non_audio(monkeypatch):
    row = SimpleNamespace(message_id="m1", s3_key="key1", mime="application/pdf")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock(return_value={"data": b"pdf", "mime": "application/pdf"}))
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    transcribe_mock = Mock()
    monkeypatch.setattr(celery_tasks, "transcribe", transcribe_mock)

    result = await celery_tasks._asr_task("att-2")

    assert result is None
    transcribe_mock.assert_not_called()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_asr_task_dedupes_existing(monkeypatch):
    row = SimpleNamespace(message_id="m2", s3_key="key2", mime="audio/wav")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.get_last_event_result = {"text": "cached"}
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock())
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    transcribe_mock = AsyncMock()
    monkeypatch.setattr(celery_tasks, "transcribe", transcribe_mock)

    result = await celery_tasks._asr_task("att-3")

    assert result == {"text": "cached"}
    transcribe_mock.assert_not_awaited()
    storage.get.assert_not_awaited()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_asr_task_happy_path(monkeypatch):
    row = SimpleNamespace(message_id="m3", s3_key="key3", mime="audio/wav")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock(return_value={"data": b"wav", "mime": "audio/wav"}))
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    transcribe_mock = AsyncMock(return_value=SimpleNamespace(text="hi there", confidence=0.9))
    monkeypatch.setattr(celery_tasks, "transcribe", transcribe_mock)

    result = await celery_tasks._asr_task("att-4")

    assert result == "hi there"
    transcribe_mock.assert_awaited_once()
    assert len(repo.events) == 1
    ticket_id, message_id, type_, payload = repo.events[0]
    assert ticket_id is None
    assert message_id == "m3"
    assert type_ == "ASR_DONE"
    assert payload["text"] == "hi there"
    session.commit.assert_awaited_once()


@pytest.mark.anyio
async def test_docqa_task_missing_attachment(monkeypatch):
    session = _make_session(first_value=None)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)

    result = await celery_tasks._docqa_task("att-1")

    assert result is None
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_docqa_task_skip_non_image(monkeypatch):
    row = SimpleNamespace(message_id="m1", s3_key="key1", mime="audio/wav")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock(return_value={"data": b"wav", "mime": "audio/wav"}))
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    extract_fields_mock = AsyncMock()
    monkeypatch.setattr(celery_tasks, "extract_fields", extract_fields_mock)

    result = await celery_tasks._docqa_task("att-2")

    assert result is None
    extract_fields_mock.assert_not_awaited()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_docqa_task_dedupes_existing(monkeypatch):
    row = SimpleNamespace(message_id="m2", s3_key="key2", mime="application/pdf")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.get_last_event_result = {"fields": {"order_id": "cached"}}
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock())
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    extract_fields_mock = AsyncMock()
    monkeypatch.setattr(celery_tasks, "extract_fields", extract_fields_mock)

    result = await celery_tasks._docqa_task("att-3")

    assert result == {"fields": {"order_id": "cached"}}
    extract_fields_mock.assert_not_awaited()
    storage.get.assert_not_awaited()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_docqa_task_happy_path(monkeypatch):
    row = SimpleNamespace(message_id="m3", s3_key="key3", mime="application/pdf")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    storage = SimpleNamespace(get=AsyncMock(return_value={"data": b"pdfdata", "mime": "application/pdf"}))
    monkeypatch.setattr(celery_tasks, "AttachmentStorage", lambda: storage)
    extract_fields_mock = AsyncMock(return_value=SimpleNamespace(model_dump=lambda: {"order_id": "DOC-1"}))
    monkeypatch.setattr(celery_tasks, "extract_fields", extract_fields_mock)

    result = await celery_tasks._docqa_task("att-4")

    assert result["fields"] == {"order_id": "DOC-1"}
    extract_fields_mock.assert_awaited_once()
    storage.get.assert_awaited_once()
    assert len(repo.events) == 1
    ticket_id, message_id, type_, payload = repo.events[0]
    assert ticket_id is None
    assert message_id == "m3"
    assert type_ == "DOCQA_DONE"
    assert payload["fields"] == {"order_id": "DOC-1"}
    session.commit.assert_awaited_once()


@pytest.mark.anyio
async def test_classify_task_missing_message(monkeypatch):
    session = _make_session(first_value=None)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    classify_mock = AsyncMock()
    monkeypatch.setattr(celery_tasks, "classify", classify_mock)

    result = await celery_tasks._classify_task("m-none")

    assert result is None
    classify_mock.assert_not_awaited()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_classify_task_dedupes_existing(monkeypatch):
    row = SimpleNamespace(id="m1", body_text="hello")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.event_by_type = {"CLASSIFY_DONE": {"label": "cached"}}
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    classify_mock = AsyncMock()
    monkeypatch.setattr(celery_tasks, "classify", classify_mock)

    result = await celery_tasks._classify_task("m1")

    assert result == {"label": "cached"}
    classify_mock.assert_not_awaited()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_classify_task_concat_asr_and_inserts(monkeypatch):
    row = SimpleNamespace(id="m2", body_text="body")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.event_by_type = {
        "CLASSIFY_DONE": None,
        "ASR_DONE": {"text": " from asr"},
    }
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    classify_mock = AsyncMock(return_value=SimpleNamespace(label="refund", scores={"refund": 0.9}))
    monkeypatch.setattr(celery_tasks, "classify", classify_mock)

    result = await celery_tasks._classify_task("m2")

    assert result["label"] == "refund"
    classify_mock.assert_awaited_once_with("body\n from asr")
    assert len(repo.events) == 1
    ticket_id, message_id, type_, payload = repo.events[0]
    assert ticket_id is None
    assert message_id == "m2"
    assert type_ == "CLASSIFY_DONE"
    assert payload["label"] == "refund"
    assert payload["scores"] == {"refund": 0.9}
    session.commit.assert_awaited_once()


@pytest.mark.anyio
async def test_normalize_task_missing_message(monkeypatch):
    session = _make_session(first_value=None)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    merge_mock = Mock()
    monkeypatch.setattr(celery_tasks, "merge_fields", merge_mock)

    result = await celery_tasks._normalize_task("m-none")

    assert result is None
    merge_mock.assert_not_called()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_normalize_task_dedupes_existing(monkeypatch):
    row = SimpleNamespace(id="m3", body_text="body")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.event_by_type = {"NORMALIZE_DONE": {"normalized": {"order_id": "cached"}}}
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    merge_mock = Mock()
    monkeypatch.setattr(celery_tasks, "merge_fields", merge_mock)

    result = await celery_tasks._normalize_task("m3")

    assert result == {"normalized": {"order_id": "cached"}}
    merge_mock.assert_not_called()
    assert repo.events == []
    session.commit.assert_not_awaited()


@pytest.mark.anyio
async def test_normalize_task_builds_docfields_and_inserts(monkeypatch):
    row = SimpleNamespace(id="m4", body_text="body")
    session = _make_session(first_value=row)
    monkeypatch.setattr(celery_tasks, "SessionLocal", lambda: session)
    repo = _FakeRepo()
    repo.event_by_type = {
        "DOCQA_DONE": {
            "fields": {
                "order_id": "DOCQA-1",
                "amount": None,
                "currency": None,
                "order_date": None,
                "sku": None,
                "confidence": {},
            }
        },
        "ASR_DONE": {"text": "from asr"},
    }
    monkeypatch.setattr(celery_tasks, "MessageRepository", lambda s: repo)
    merged = SimpleNamespace(model_dump=lambda: {"order_id": "FINAL"})
    merge_mock = Mock(return_value=merged)
    monkeypatch.setattr(celery_tasks, "merge_fields", merge_mock)

    result = await celery_tasks._normalize_task("m4")

    assert result["normalized"] == {"order_id": "FINAL"}
    merge_mock.assert_called_once()
    args, kwargs = merge_mock.call_args
    doc_fields_arg, body_text_arg, transcript_arg = args
    assert doc_fields_arg.order_id == "DOCQA-1"
    assert body_text_arg == "body"
    assert transcript_arg == "from asr"
    assert len(repo.events) == 1
    ticket_id, message_id, type_, payload = repo.events[0]
    assert ticket_id is None
    assert message_id == "m4"
    assert type_ == "NORMALIZE_DONE"
    assert payload["normalized"] == {"order_id": "FINAL"}
    session.commit.assert_awaited_once()
