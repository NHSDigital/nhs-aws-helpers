import asyncio
from functools import partial
from typing import Callable, Final, TypeVar, cast

KiB: Final[int] = 1024
MiB: Final[int] = KiB * 1024
GiB: Final[int] = MiB * 1024


TReturn = TypeVar("TReturn")


async def run_in_executor(func: Callable[..., TReturn], *args, **kwargs) -> TReturn:
    """
        async wrapper for sync code
    Args:
        func: the function to call
        *args: positional args to pass to fund
        **kwargs: kwargs to pass to func

    Returns:

    """
    loop = asyncio.get_running_loop()

    to_execute = partial(func, *args, **kwargs)
    result = cast(TReturn, await loop.run_in_executor(None, to_execute))

    return result
