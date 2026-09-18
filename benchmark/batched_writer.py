"""Benchmark the effect of batching and buffering on a writer."""

import time
import tempfile
from pathlib import Path
from pystow.utils import safe_open_writer
import click
import pandas as pd
from tqdm import trange
from tqdm.contrib.itertools import product


def _generate_data(total: int = 1_000_000) -> list[tuple[str, ...]]:
    return [
        tuple(str(i + j) for j in range(10))
        for i in trange(total, desc='Generating data', unit_scale=True)
    ]


def main():
    results = []
    data = _generate_data()
    extensions = [".tsv.gz", ".tsv"]
    bufferings = [None, 1024, 1024 ** 2, 2 * 1024 ** 2]
    batch_sizes = [None, 1_000, 10_000, 100_000]
    groupeds = [True, False]

    for ext, buffering, batch_size, grouped in product(extensions, bufferings, batch_sizes, groupeds):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir).joinpath("batched_writer").with_suffix(ext)
            start = time.time()
            with safe_open_writer(path, buffering=buffering, batch_size=batch_size) as writer:
                if grouped:
                    writer.writerows(data)
                else:
                    for row in data:
                        writer.writerow(row)
            delta = time.time() - start
            results.append((buffering or 0, batch_size or 0, grouped, ext, delta))

    df = pd.DataFrame(results, columns=["buffering", "batch_size", "grouped", "extension", "delta"])
    df.to_csv("results.tsv", sep="\t", index=False)
    click.echo(df.to_markdown(tablefmt="github", index=False))


if __name__ == '__main__':
    main()
