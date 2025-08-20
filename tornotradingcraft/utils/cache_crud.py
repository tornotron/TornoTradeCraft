import pandas as pd
import io
import logging
from typing import Optional
from diskcache import Cache
from tornotradingcraft.utils.cache import get_diskcache

# Get a logger for this module
log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

# Get cache instance from the Tornotradingcraft cache module
cache: Cache = get_diskcache()

def save_df_to_cache(key: str, df: pd.DataFrame) -> None:
    """
    Serializes a pandas DataFrame to Parquet format and stores it in the cache.

    Args:
        key: The unique key to identify the cached item.
        df: The pandas DataFrame to store.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        log.warning(f"Attempted to cache an empty or invalid DataFrame for key '{key}'. Skipping.")
        return

    try:
        # Use an in-memory buffer to write the DataFrame to Parquet format
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        
        # Store the raw bytes from the buffer into the cache
        cache.set(key, buffer.getvalue())
        log.info(f"Successfully saved DataFrame to cache with key '{key}'.")
    except Exception:
        log.error(f"Failed to save DataFrame to cache for key '{key}'.", exc_info=True)

def load_df_from_cache(key: str) -> Optional[pd.DataFrame]:
    """
    Loads a pandas DataFrame from the cache if it exists.

    Args:
        key: The unique key to identify the cached item.

    Returns:
        A pandas DataFrame if the key is found, otherwise None.
    """
    cached_bytes = cache.get(key)

    if cached_bytes and isinstance(cached_bytes, bytes):
        if isinstance(cached_bytes, bytes):
            try:
                # Use an in-memory BytesIO buffer to read the Parquet data
                buffer = io.BytesIO(cached_bytes)
                log.info(f"Loading DataFrame from cache with key '{key}'.")
                return pd.read_parquet(buffer)
            except Exception:
                log.error(f"Failed to load/decode DataFrame from cache for key '{key}'.", exc_info=True)
                return None
        else:
            # Handle unexpected cached data type
            log.warning(f"Cached data for key '{key}' is not bytes (type: {type(cached_bytes)}). Cannot load DataFrame.")
            return None
    elif cached_bytes is None:
        log.debug(f"Cache miss for key '{key}'.")
        return None
    else:
        log.debug(f"Cache miss for key '{key}'.")
        return None    