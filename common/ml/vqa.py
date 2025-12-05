from transformers import pipeline
from PIL import Image
import io

from . import use_stub

import anyio

_vqa = None


def _get_vqa():
    global _vqa
    if _vqa is None:
        _vqa = pipeline(
            "image-classification",
            model="google/vit-base-patch16-224-in21k",
            device="cpu",
        )
    return _vqa


def is_damaged_sync(image_bytes: bytes) -> bool:
    if use_stub():
        return False

    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    preds = _get_vqa()(img)
    damage_keywords = {
        "broken",
        "crack",
        "cracked",
        "dent",
        "scratched",
        "scratch",
        "torn",
        "rip",
        "ripped",
        "defect",
        "damaged",
        "damage",
        "bent",
        "shattered",
    }
    for pred in preds:
        label = pred.get("label", "").lower()
        score = float(pred.get("score", 0.0))
        if score < 0.3:
            continue
        if any(k in label for k in damage_keywords):
            return True

    return False


async def is_damaged(image_bytes: bytes) -> bool:
    return await anyio.to_thread.run_sync(
        is_damaged_sync,
        image_bytes,
    )
