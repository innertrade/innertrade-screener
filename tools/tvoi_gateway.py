#!/usr/bin/env python3
"""Utility helpers for interacting with the TVOI HTTP gateway."""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

DEFAULT_GATEWAY = "http://127.0.0.1:8787"


def _request_json(url: str, data: Dict[str, Any] | None = None) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:  # noqa: S310 - localhost only
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:  # pragma: no cover - convenience script
        print(exc.read().decode("utf-8"), file=sys.stderr)
        raise
    return json.loads(payload or "{}")


def cmd_health(args: argparse.Namespace) -> None:
    url = f"{args.gateway.rstrip('/')}/health"
    payload = _request_json(url)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def cmd_send(args: argparse.Namespace) -> None:
    if args.file:
        data = json.loads(Path(args.file).read_text(encoding="utf-8"))
    elif args.data:
        data = json.loads(args.data)
    else:
        data = {
            "type": "PRE",
            "symbol": "TESTUSDT",
            "tf": "5m",
            "price": 123.45,
            "link": "https://example.com",
        }
    url = f"{args.gateway.rstrip('/')}/tvoi"
    payload = _request_json(url, data=data)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY, help="Gateway base URL (default: %(default)s)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    health_parser = subparsers.add_parser("health", help="Query the /health endpoint")
    health_parser.set_defaults(func=cmd_health)

    send_parser = subparsers.add_parser("send", help="POST a payload to /tvoi")
    send_parser.add_argument("--file", type=str, help="Path to JSON file to send")
    send_parser.add_argument("--data", type=str, help="Raw JSON string to send")
    send_parser.set_defaults(func=cmd_send)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except urllib.error.URLError as exc:
        print(f"request failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
