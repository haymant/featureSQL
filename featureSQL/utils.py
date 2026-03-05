"""General utility helpers used across the package."""

import time
from loguru import logger


def deco_retry(retry: int = 5, retry_sleep: int = 3):
    """Decorator to retry a function on exception.

    Parameters
    ----------
    retry : int
        Number of attempts before giving up.
    retry_sleep : int
        Seconds to sleep between attempts.
    """
    def deco_func(func):
        def wrapper(*args, **kwargs):
            _retry = retry
            for i in range(1, _retry + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"{func.__name__}: {i} {e}")
                    if i == _retry:
                        raise
                    time.sleep(retry_sleep)
        return wrapper
    return deco_func
