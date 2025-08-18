from appdirs import user_cache_dir
from pathlib import Path
from diskcache import Cache
from typing import Optional
import atexit
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

APP_NAME = "TornoTradeCraft"
APP_AUTHOR = "Dr.Abhijith Anandakrishnan"

# This creates a path like ".../TornoTradeCraft/cache" in the user's cache directory
CACHE_DIR = Path(user_cache_dir(APP_NAME, APP_AUTHOR)) / "cache"

_cache: Optional[Cache] = None

def ensure_cache_dir(mode: int = 0o755) -> Path:
    """
    Ensure the cache directory exists and return it.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True, mode=mode)
    return CACHE_DIR


def get_cache_dir(create: bool = True) -> Path:
    """
    Return the cache directory Path. If create is True the directory will be created.
    """
    return ensure_cache_dir() if create else CACHE_DIR


def get_diskcache() -> Cache:
    global _cache
    if _cache is None:
        cache_dir = get_cache_dir()
        logger.debug(f"Initializing cache at {cache_dir}")
        _cache = Cache(
            cache_dir,
            shard_limit=1000,
            size_limit=10**9,
            eviction_policy='least-recently-used',
            disk_cache=True,
            timeout=60*60*24*7,
        )
        atexit.register(lambda: _cache.close() if _cache is not None else None)
    return _cache


__all__ = ["get_diskcache"]