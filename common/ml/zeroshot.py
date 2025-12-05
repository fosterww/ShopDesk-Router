from transformers import pipeline

from . import use_stub
from .types import Classification

import anyio

LABELS = ["refund", "not_received", "warranty", "address_change", "how_to", "other"]

_zs_pipeline = None


def _get_zs():
    global _zs_pipeline
    if _zs_pipeline is None:
        _zs_pipeline = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
        )
    return _zs_pipeline


def classify_sync(text: str) -> Classification:
    if use_stub():
        ...

    zs = _get_zs()
    result = zs(text, LABELS)
    labels = result["labels"]
    scores = result["scores"]
    best = labels[0]
    return Classification(
        label=best,
        scores=dict(zip(labels, scores)),
    )


async def classify(text: str) -> Classification:
    return await anyio.to_thread.run_sync(
        classify_sync,
        text,
    )
