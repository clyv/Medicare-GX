"""
probe_schema.py
Reads just the header of a remote CSV and prints its columns.

Writing a contract against guessed column names is how you get a red build
that tells you nothing. This fetches the first few KB with a ranged request
and prints the real header, so a new CMS extract can be described accurately
before a single expectation is written for it.

    python pipelines/probe_schema.py <url>
    python pipelines/probe_schema.py <url> --rows 3   # also show sample rows

Output is a Python list literal, ready to paste into a contract module.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import textwrap

import requests

# Enough for a header plus a few rows; CMS headers run to a few kilobytes.
DEFAULT_BYTES = 64 * 1024


def fetch_head(url: str, num_bytes: int) -> str:
    response = requests.get(
        url, headers={"Range": f"bytes=0-{num_bytes - 1}"}, timeout=120, stream=True
    )
    # A server that ignores Range returns 200 and the whole file; stop reading
    # once we have what we asked for either way.
    response.raise_for_status()

    buffer = io.BytesIO()
    for chunk in response.iter_content(chunk_size=8192):
        buffer.write(chunk)
        if buffer.tell() >= num_bytes:
            break
    response.close()

    return buffer.getvalue().decode("utf-8-sig", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="CSV url to probe")
    parser.add_argument("--rows", type=int, default=0, help="sample rows to print")
    parser.add_argument("--bytes", type=int, default=DEFAULT_BYTES)
    args = parser.parse_args()

    text = fetch_head(args.url, args.bytes)
    reader = csv.reader(io.StringIO(text))

    try:
        header = next(reader)
    except StopIteration:
        print("[ERROR] no header found", file=sys.stderr)
        return 1

    print(f"[URL]     {args.url}")
    print(f"[COLUMNS] {len(header)}\n")

    for index, column in enumerate(header, start=1):
        print(f"  {index:>3}. {column}")

    print("\n[LITERAL]")
    print(
        textwrap.fill(
            ", ".join(repr(c) for c in header),
            96,
            initial_indent="    ",
            subsequent_indent="    ",
        )
    )

    for row_number in range(args.rows):
        try:
            row = next(reader)
        except StopIteration:
            break
        print(f"\n[ROW {row_number + 1}]")
        for column, value in zip(header, row):
            print(f"  {column:<34} {value}")

    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
