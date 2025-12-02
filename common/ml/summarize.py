from transformers import pipeline

from . import use_stub
from .types import Summary

import anyio

_sum_pipeline = None


def _get_sum():
    global _sum_pipeline
    if _sum_pipeline is None:
        _sum_pipeline = pipeline(
            "summarization",
            model="facebook/bart-large-cnn",
        )
    return _sum_pipeline


def summarize_sync(text: str, max_chars: int = 480) -> Summary:
    if use_stub():
        txt = (
            "Customer reports damaged item in order A10023. "
            "Proposed refund prepared and waiting for approval."
        )
        return Summary(text=txt, tokens=len(txt.split()))

    summ = _get_sum()
    res = summ(
        text,
        max_length=120,
        min_length=40,
        do_sample=False,
    )
    out = res[0]["summary_text"]
    if len(out) > max_chars:
        out = out[: max_chars - 3] + "..."
    return Summary(text=out, tokens=len(out.split()))


async def summarize(text: str, max_chars: int = 480):
    return await anyio.to_thread.run_sync(
        summarize_sync,
        text,
        max_chars,
    )
