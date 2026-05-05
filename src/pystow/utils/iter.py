"""Utilities for functions that consume iterables."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    X = TypeVar("X")

__all__ = [
    "reyield",
]


class Sentinel:
    """A sentinel class."""


_SENTINEL = Sentinel()


def reyield(func: Callable[[Iterable[X]], None], elements: Iterable[X]) -> Generator[X, None, None]:
    """Make a function that consumes an iterable yield its elements.

    :param func: A function that consumes an iterable, and does not return anything
    :param elements: An iterable

    :returns: A generator that injects the elements of the iterable one at a time before
        yielding them.

    >>> state = {"value": 0}
    >>> def accumulate(elements: Iterable[int]) -> None:
    ...     for element in elements:
    ...         state["value"] += element
    >>> square_sum = 0
    >>> for element in reyield(accumulate, range(5)):
    ...     square_sum += element**2

    The goal of this function is not to need to keep elements in memory, so this should
    be an iterable-native alternative to just doing:

    >>> elements = range(5)
    >>> elements = list(elements)
    >>> accumulate(elements)
    >>> square_sum = 0
    >>> for element in elements:
    ...     square_sum += element**2
    """
    from contextlib import closing

    with closing(_help_reiter(func)) as generator:
        next(generator)  # prime the coroutine
        for element in elements:
            generator.send(element)
            yield element


def _help_reiter(func: Callable[[Iterable[X]], None]) -> Generator[None, X, None]:
    """Modify the function."""
    import threading
    from queue import Queue

    queue: Queue[X | Sentinel] = Queue()

    def _iterable_from_queue() -> Generator[X, None, None]:
        while True:
            # queue.get() blocks indefinitely (because timeout is None)
            # until the queue receives something
            item_ = queue.get(block=True, timeout=None)

            # if the queue receives a sentinel value, then we break out of the
            # while loop, which will cause the generator to raise a GeneratorExit
            if isinstance(item_, Sentinel):
                break
            yield item_

    # Run the consumer in a background thread, fed by the generator that wraps the queue
    thread = threading.Thread(target=func, args=(_iterable_from_queue(),), daemon=True)
    thread.start()

    # now, we invert the generator pattern - this function
    # will return a generator that you can .send(item) to,
    # and the yield statement waits for the value and sticks
    # it in item
    try:
        while True:
            item: X = yield  # pause, wait for .send(item)
            queue.put(item)
    except GeneratorExit:
        # this happens when the outer generator ends, which means
        # it's time to send the queue a "final" value and block
        # on finishing the thread to clean up
        pass
    finally:
        queue.put(_SENTINEL)  # signal the consumer that we're done
        thread.join()
