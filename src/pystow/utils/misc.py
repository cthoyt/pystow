"""Miscellaneous utility functions."""

__all__ = [
    "get_exception_str",
]


def get_exception_str(exc: Exception) -> str:
    """Get the traceback of an exception as a string."""
    import traceback
    from io import StringIO

    file = StringIO()
    traceback.print_exception(exc, file=file)
    file.seek(0)
    return file.read()
