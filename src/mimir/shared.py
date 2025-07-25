import functools
import time


def ttl_cache(ttl_seconds: int):
    """
    A decorator to add a time-to-live (TTL) to a memoized function.
    It uses functools.lru_cache as the underlying caching mechanism.
    """

    def decorator(func):
        # Create a cached version of the function with an unlimited cache size.
        # The TTL is managed by the time-dependent '_ttl_hash' argument.
        @functools.lru_cache(maxsize=None)
        def _cached_func(*args, _ttl_hash, **kwargs):  # type: ignore[unused-ignore]
            return func(*args, **kwargs)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            Calculates the current time bucket and calls the cached function.
            """
            # Calculate the time bucket. This value is constant for the duration of the TTL.
            # For example, with ttl=60, this will be the same for a full minute.
            ttl_hash = int(time.time() / ttl_seconds)

            # Call the cached function with the time bucket as a keyword argument.
            return _cached_func(*args, _ttl_hash=ttl_hash, **kwargs)

        return wrapper

    return decorator
