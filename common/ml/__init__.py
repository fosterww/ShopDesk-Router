import os


def use_stub() -> bool:
    # Default to real models; set ML_MODE=stub to force stubs
    return os.getenv("ML_MODE", "real").lower() == "stub"
