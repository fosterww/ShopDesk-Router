from celery import shared_task

from common.ml.docqa import extract_fields
from common.ml.asr import transcribe
from common.ml.zeroshot import classify
from common.ml.docqa import extract_fields
from common.ml.summarize import summarize
from common.ml.vqa import is_damaged
