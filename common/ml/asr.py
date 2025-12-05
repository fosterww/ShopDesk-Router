from transformers import pipeline

from . import use_stub
from .types import Transcript

import anyio

import io, soundfile as sf

_asr_pipeline = None


def _get_asr():
    global _asr_pipeline
    if _asr_pipeline is None:
        _asr_pipeline = pipeline(
            "automatic-speech-recognition",
            model="openai/whisper-tiny",
            device="cpu",
            chunk_length_s=30,
            generate_kwargs={"task": "transcribe", "language": "en"},
        )
    return _asr_pipeline


def transcribe_sync(audio_bytes: bytes, mime: str) -> Transcript:
    if use_stub():
        ...

    data, sr = sf.read(io.BytesIO(audio_bytes))
    asr = _get_asr()
    result = asr({"array": data, "sampling_rate": sr})
    return Transcript(text=result["text"], confidence=float(result.get("score", 1.0)))


async def transcribe(audio_bytes: bytes, mime: str) -> Transcript:
    return await anyio.to_thread.run_sync(
        transcribe_sync,
        audio_bytes,
        mime,
    )
