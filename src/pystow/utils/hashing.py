"""Hashing utilities."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Collection, Iterable, Mapping
from pathlib import Path
from typing import NamedTuple, TypeAlias

import requests

__all__ = [
    "Hash",
    "HexDigestError",
    "HexDigestMismatch",
    "get_hash_hexdigest",
    "get_hashes",
    "get_hexdigests_remote",
    "get_offending_hexdigests",
    "raise_on_digest_mismatch",
]

logger = logging.getLogger(__name__)

Hash: TypeAlias = "hashlib._Hash"


def get_hexdigests_remote(
    hexdigests_remote: Mapping[str, str] | None, hexdigests_strict: bool = False
) -> Mapping[str, str]:
    """Process hexdigests via URLs.

    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hex digest) pairs.
    :param hexdigests_strict: Set this to `False` to stop automatically checking for the
        `algorithm(filename)=hash` format

    :returns: A mapping of algorithms to hexdigests
    """
    rv = {}
    for key, url in (hexdigests_remote or {}).items():
        text = requests.get(url, timeout=15).text
        if not hexdigests_strict and "=" in text:
            text = text.rsplit("=", 1)[-1].strip()
        rv[key] = text
    return rv


class HexDigestMismatch(NamedTuple):
    """Contains information about a hexdigest mismatch."""

    #: the name of the algorithm
    name: str
    #: the observed/actual hexdigest, encoded as a string
    actual: str
    #: the expected hexdigest, encoded as a string
    expected: str


def get_offending_hexdigests(
    path: str | Path,
    chunk_size: int | None = None,
    hexdigests: Mapping[str, str] | None = None,
    hexdigests_remote: Mapping[str, str] | None = None,
    hexdigests_strict: bool = False,
) -> Collection[HexDigestMismatch]:
    """Check a file for hash sums.

    :param path: The file path.
    :param chunk_size: The chunk size for reading the file.
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.
    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hexdigest) pairs.
    :param hexdigests_strict: Set this to false to stop automatically checking for the
        `algorithm(filename)=hash` format

    :returns: A collection of observed / expected hexdigests where the digests do not
        match.
    """
    hexdigests = dict(
        **(hexdigests or {}),
        **get_hexdigests_remote(hexdigests_remote, hexdigests_strict=hexdigests_strict),
    )

    # If there aren't any keys in the combine dictionaries,
    # then there won't be any mismatches
    if not hexdigests:
        return []

    logger.info(f"Checking hash sums for file: {path}")

    # instantiate algorithms
    algorithms = get_hashes(path=path, names=set(hexdigests), chunk_size=chunk_size)

    # Compare digests
    mismatches = []
    for alg, expected_digest in hexdigests.items():
        observed_digest = algorithms[alg].hexdigest()
        if observed_digest != expected_digest:
            logger.error(f"{alg} expected {expected_digest} but got {observed_digest}.")
            mismatches.append(HexDigestMismatch(alg, observed_digest, expected_digest))
        else:
            logger.debug(f"Successfully checked with {alg}.")

    return mismatches


def get_hashes(
    path: str | Path,
    names: Iterable[str],
    *,
    chunk_size: int | None = None,
) -> Mapping[str, Hash]:
    """Calculate several hexdigests of hash algorithms for a file concurrently.

    :param path: The file path.
    :param names: Names of the hash algorithms in :mod:`hashlib`
    :param chunk_size: The chunk size for reading the file.

    :returns: A collection of observed hexdigests
    """
    path = Path(path).resolve()
    if chunk_size is None:
        chunk_size = 64 * 2**10

    # instantiate hash algorithms
    algorithms: Mapping[str, Hash] = {name: hashlib.new(name) for name in names}

    # calculate hash sums of file incrementally
    buffer = memoryview(bytearray(chunk_size))
    with path.open("rb", buffering=0) as file:
        for this_chunk_size in iter(lambda: file.readinto(buffer), 0):
            for alg in algorithms.values():
                alg.update(buffer[:this_chunk_size])

    return algorithms


def get_hash_hexdigest(
    path: str | Path,
    name: str,
    *,
    chunk_size: int | None = None,
) -> str:
    """Get a hash digest for a single hash."""
    r = get_hashes(path, [name], chunk_size=chunk_size)
    return r[name].hexdigest()


def raise_on_digest_mismatch(
    *,
    path: Path,
    hexdigests: Mapping[str, str] | None = None,
    hexdigests_remote: Mapping[str, str] | None = None,
    hexdigests_strict: bool = False,
) -> None:
    """Raise a HexDigestError if the digests do not match.

    :param path: The file path.
    :param hexdigests: The expected hexdigests as (algorithm_name, expected_hex_digest)
        pairs.
    :param hexdigests_remote: The expected hexdigests as (algorithm_name, url to file
        with expected hexdigest) pairs.
    :param hexdigests_strict: Set this to false to stop automatically checking for the
        `algorithm(filename)=hash` format

    :raises HexDigestError: if there are any offending hex digests The expected
        hexdigests as (algorithm_name, url to file with expected hexdigest) pairs.
    """
    offending_hexdigests = get_offending_hexdigests(
        path=path,
        hexdigests=hexdigests,
        hexdigests_remote=hexdigests_remote,
        hexdigests_strict=hexdigests_strict,
    )
    if offending_hexdigests:
        raise HexDigestError(offending_hexdigests)


class HexDigestError(ValueError):
    """Thrown if the hashsums do not match expected hashsums."""

    def __init__(self, offending_hexdigests: Collection[HexDigestMismatch]):
        """Instantiate the exception.

        :param offending_hexdigests: The result from :func:`get_offending_hexdigests`
        """
        self.offending_hexdigests = offending_hexdigests

    def __str__(self) -> str:
        return "\n".join(
            (
                "Hexdigest of downloaded file does not match the expected ones!",
                *(
                    f"\t{name} actual: {actual} vs. expected: {expected}"
                    for name, actual, expected in self.offending_hexdigests
                ),
            )
        )
