#!/usr/bin/env python3
"""Helper CLI to send TVOI payloads to the local gateway."""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

DEFAULT_SAMPLE = {
    "type": "PRE",
    "symbol": "TESTUSDT",
    "tf": "5m",
    "price": 123,
    "link": "http://example.com",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8787/tvoi",
        help="Gateway URL (default: %(default)s)",
    )
    parser.add_argument(
        "--payload",
        help="Raw JSON payload to send. Overrides the default sample.",
    )
    parser.add_argument(
        "--file",
        help="Path to a JSON file with the payload to send.",
    )
    return parser.parse_args()


def load_payload(args: argparse.Namespace) -> dict:
    if args.file:
        with open(args.file, "r", encoding="utf-8") as handle:
            return json.load(handle)
    if args.payload:
        return json.loads(args.payload)
    return DEFAULT_SAMPLE


def main() -> int:
    args = parse_args()
    payload = load_payload(args)
    data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        args.url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            print(body)
            return 0
    except urllib.error.HTTPError as exc:  # pragma: no cover - CLI surface
        print(exc.read().decode("utf-8"), file=sys.stderr)
        return exc.code
    except Exception as exc:  # pragma: no cover - CLI surface
        print(f"request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
