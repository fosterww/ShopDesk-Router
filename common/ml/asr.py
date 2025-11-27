from transformers import pipeline

from . import use_stub
from .types import Transcript

import anyio

_asr_pipeline = None

def _get_asr():
    global _asr_pipeline
    if _asr_pipeline is None:
        _asr_pipeline = pipeline(
            "automatic-speech-recognition",
            model="openai/whisper-tiny",
        )
    return _asr_pipeline

def transcribe_sync(audio_bytes: bytes, mime: str) -> Transcript:
    if use_stub():
        return Transcript(
            text="Hello, i need a refund for order A10023.",
            confidence=0.97,
        )

    asr = _get_asr()
    result = asr(audio_bytes)
    return Transcript(
        text=result["text"],
        confidence=float(result.get("confidence", 1.0)),
    )

async def transcribe(audio_bytes: bytes, mime: str) -> Transcript:
    return await anyio.to_thread.run_sync(
        transcribe_sync,
        audio_bytes,
        mime,
    )