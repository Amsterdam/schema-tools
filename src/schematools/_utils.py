"""Internal utils, not meant to be used outside schematools."""

from __future__ import annotations

import functools
import weakref


def cached_method(*lru_args, **lru_kwargs):
    """A simple lru-cache per object.
    This removed the need for methodtools.lru_cache(), which uses wirerope for purity.
    The usage of wirerope started showing up as 5% of the request time,
    hence it's significant to remove.
    """

    def decorator(func):
        @functools.wraps(func)
        def initial_wrapped_func(self, *args, **kwargs):
            # Not storing the wrapped method inside the instance. If we had
            # a strong reference to self the instance would never die.
            self_weak = weakref.ref(self)

            @functools.wraps(func)
            @functools.lru_cache(*lru_args, **lru_kwargs)
            def cached_method(*args, **kwargs):
                return func(self_weak(), *args, **kwargs)

            # Assigns to the self reference (preserving the cache), and optimizes the next access.
            setattr(self, func.__name__, cached_method)
            return cached_method(*args, **kwargs)

        return initial_wrapped_func

    return decorator
