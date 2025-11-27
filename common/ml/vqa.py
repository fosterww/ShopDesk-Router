from transformers import pipeline

from . import use_stub

import anyio

_vqa = None

def _get_vqa():
    global _vqa
    if _vqa is None:
        _vqa = pipeline(
            "visual-question-answering",
            model="dandelin/vilt-b32-finetuned-vqa",
        )
    return _vqa


def is_damaged_sync(image_bytes: bytes) -> bool:
    if use_stub():
        return True
    
    vqa = _get_vqa()
    out = vqa(
        image=image_bytes,
        question="Is the package or box damaged?",
    )[0]
    answer = out["answer"].lower()
    return answer in {"yes", "damaged", "crushed"}

async def is_damaged(image_bytes: bytes) -> bool:
    return await anyio.to_thread.run_sync(
        is_damaged_sync,
        image_bytes,
    )
