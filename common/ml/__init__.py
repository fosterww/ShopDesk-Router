import os

ML_MODE = os.getenv("ML_MODE", "stub")

def use_stub() -> bool:
    return ML_MODE == "stub"