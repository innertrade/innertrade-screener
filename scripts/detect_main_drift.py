#!/usr/bin/env python3
"""Check that local main.py matches origin/main and contains critical markers."""
from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = REPO_ROOT / "main.py"

REQUIRED_TOKEN = "\"intervalTime\""
FORBIDDEN_TOKEN = '"interval": str('


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def maybe_fetch(fetch: bool, ref: str) -> None:
    if not fetch:
        return
    try:
        run_git(["fetch", "--quiet", "origin", ref.split(":")[0]])
    except subprocess.CalledProcessError as exc:
        print("[WARN] git fetch failed:", exc.stderr.strip(), file=sys.stderr)
        sys.exit(2)


def load_ref_blob(ref: str) -> Optional[bytes]:
    try:
        result = run_git(["show", f"{ref}:main.py"])
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] cannot load {ref}:main.py", file=sys.stderr)
        print(exc.stderr.strip(), file=sys.stderr)
        return None
    return result.stdout.encode()


def validate_tokens(content: str) -> list[str]:
    problems: list[str] = []
    if REQUIRED_TOKEN not in content:
        problems.append("missing intervalTime token")

    oi_idx = content.find("def _oi_series_bybit")
    if oi_idx == -1:
        problems.append("_oi_series_bybit not found")
        return problems

    next_def = content.find("\ndef ", oi_idx + 1)
    if next_def == -1:
        slice_ = content[oi_idx:]
    else:
        slice_ = content[oi_idx:next_def]

    if REQUIRED_TOKEN not in slice_:
        problems.append("intervalTime token missing inside _oi_series_bybit")
    if FORBIDDEN_TOKEN in slice_:
        problems.append("found forbidden interval wrapper token inside _oi_series_bybit")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ref",
        default="origin/main",
        help="git ref to compare with (default: origin/main)",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="fetch the ref from origin before comparing",
    )
    args = parser.parse_args()

    if not MAIN_PATH.exists():
        print(f"[ERROR] {MAIN_PATH} not found", file=sys.stderr)
        return 2

    maybe_fetch(args.fetch, args.ref)
    ref_blob = load_ref_blob(args.ref)
    if ref_blob is None:
        return 2

    local_data = MAIN_PATH.read_bytes()
    local_sha = sha256(local_data)
    ref_sha = sha256(ref_blob)

    print(f"local sha256:  {local_sha}")
    print(f"{args.ref} sha256: {ref_sha}")

    status = 0
    if local_sha != ref_sha:
        print("[ERROR] main.py drift detected", file=sys.stderr)
        status = 1

    token_problems = validate_tokens(local_data.decode(errors="ignore"))
    if token_problems:
        for issue in token_problems:
            print(f"[ERROR] {issue}", file=sys.stderr)
        status = 1

    if status:
        try:
            diff = run_git(["diff", f"{args.ref}", "--", "main.py"])
            if diff.stdout:
                print(diff.stdout)
        except subprocess.CalledProcessError:
            pass

    return status


if __name__ == "__main__":
    sys.exit(main())
