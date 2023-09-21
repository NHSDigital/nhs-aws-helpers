import asyncio
import typing
from dataclasses import is_dataclass
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


def is_dataclass_instance(obj) -> bool:
    return is_dataclass(obj) and not isinstance(obj, type)


_NoneType = type(None)


def optional_origin_type(original_type: type) -> type:
    """
    if the target type is Optional or a Union[xxx, None] this will return the wrapped type
    """
    if original_type.__class__.__name__ not in ("_UnionGenericAlias", "UnionType", "_GenericAlias"):
        return original_type
    args = typing.get_args(original_type)
    if len(args) != 2:
        return original_type

    args = tuple(arg for arg in args if arg != _NoneType)
    if len(args) != 1:
        return original_type
    return typing.cast(type, args[0])
