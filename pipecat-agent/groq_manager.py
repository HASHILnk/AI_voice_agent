import os
from loguru import logger

_current_index = 0


def get_next_groq_key():
    global _current_index
    groq_keys = [
        os.getenv("GROQ_API_KEY_1"),
        os.getenv("GROQ_API_KEY_2"),
        os.getenv("GROQ_API_KEY_3"),
        os.getenv("GROQ_API_KEY_4"),
        os.getenv("GROQ_API_KEY_5")
    ]
    active_keys = [(i + 1, k) for i, k in enumerate(groq_keys) if k]
    if not active_keys:
        logger.error("No active Groq API keys found in environment variables!")
        return None
    
    key_num, key = active_keys[_current_index % len(active_keys)]
    masked_key = f"...{key[-6:]}" if len(key) > 6 else "***"
    logger.info(f"Selected API Key: GROQ_API_KEY_{key_num} ({masked_key})")
    
    _current_index = (_current_index + 1) % len(active_keys)
    return key