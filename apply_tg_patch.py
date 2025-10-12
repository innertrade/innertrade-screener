# -*- coding: utf-8 -*-
"""
Идемпотентный патч для вашего main.py:
 - гарантирует `from __future__ import annotations` в самом верху
 - добавляет импорты `import logging` и `import tgwire` (если их нет)
 - вставляет единоразово `TG = tgwire.TelegramSender.from_env()`
 - после каждого логгирования '[signal]' добавляет отправку в Telegram

Запуск:
    cd /home/deploy/apps/innertrade-screener
    python3 apply_tg_patch.py

Резервная копия создаётся: main.py.tgpatch.bak.YYYYmmdd_HHMMSS
"""
from __future__ import annotations

import re
import time
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent
MAIN = ROOT / "main.py"

def read_text(p: pathlib.Path) -> str:
    return p.read_text(encoding="utf-8")

def write_text(p: pathlib.Path, s: str) -> None:
    p.write_text(s, encoding="utf-8")

def ensure_future_import(s: str) -> str:
    lines = s.splitlines(True)
    i = 0
    out = []
    # shebang
    if i < len(lines) and lines[i].startswith("#!"):
        out.append(lines[i]); i += 1
    # leading comments
    while i < len(lines) and re.match(r"\s*#", lines[i]):
        out.append(lines[i]); i += 1
    # module docstring triple
    if i < len(lines) and re.match(r'\s*("""|\'\'\')', lines[i]):
        q = lines[i].lstrip()[0]*3
        out.append(lines[i]); i += 1
        while i < len(lines):
            out.append(lines[i])
            if lines[i].rstrip().endswith(q):
                i += 1
                break
            i += 1
    # remove any broken 'from future import ...'
    tail = [ln for ln in lines[i:] if not re.match(r"\s*from\s+future\s+import\s+annotations\s*$", ln, flags=re.I)]
    # ensure correct future import once
    out.append("from __future__ import annotations\n\n")
    out.extend(tail)
    return "".join(out)

def ensure_imports(s: str) -> str:
    lines = s.splitlines(True)
    # find import block start
    i = 0
    while i < len(lines) and not re.match(r"\s*(from|import)\s+", lines[i]):
        i += 1
    j = i
    while j < len(lines) and re.match(r"\s*(from|import)\s+", lines[j]):
        j += 1
    imports = lines[i:j]
    body = lines[:i] + lines[j:]

    missing = []
    if not any(re.match(r"\s*import\s+logging(\s|$)", ln) for ln in imports):
        missing.append("import logging\n")
    if not any(re.match(r"\s*import\s+tgwire(\s|$)", ln) for ln in imports):
        missing.append("import tgwire\n")

    if missing:
        imports = imports + missing

    new_lines = lines[:i] + imports + body
    return "".join(new_lines)

def ensure_tg_init(s: str) -> str:
    # вставим TG = tgwire.TelegramSender.from_env() после импортов, если ещё нет
    if re.search(r"\bTG\s*=\s*tgwire\.TelegramSender\.from_env\(\)", s):
        return s
    m = re.search(r"(?:\n(?:from|import)\s+[^\n]+)+\n", s)
    if not m:
        # если не нашли импорт-блок, просто вставим в начало файла после future-импорта
        return re.sub(r"(from __future__ import annotations\s*\n+)", r"\1TG = tgwire.TelegramSender.from_env()\n\n", s, count=1)
    idx = m.end()
    return s[:idx] + "TG = tgwire.TelegramSender.from_env()\n" + s[idx:]

SEND_BLOCK = r"""
try:
    if TG.cfg.enabled:
        TG.send_message(_fmt_signal(sig))
    else:
        logging.getLogger("tg").warning("skip send: TG disabled (no token/chat)")
except Exception:
    logging.getLogger("tg").exception("send_tg crashed")
""".strip("\n")

def ensure_fmt_helper(s: str) -> str:
    # если уже есть _fmt_signal — не трогаем
    if re.search(r"\ndef\s*_fmt_signal\s*\(", s):
        return s
    helper = r"""
def _fmt_signal(sig: dict) -> str:
    sym = sig.get("symbol", "?")
    close = sig.get("close")
    z = sig.get("zprice")
    vm = sig.get("vol_mult")
    v24 = sig.get("vol24h_usd")
    ts = sig.get("ts")
    try:
        v24s = f"${int(float(v24)):,}".replace(",", " ")
    except Exception:
        v24s = str(v24)
    return f"⚡️ {sym}\nprice: {close}\nz: {z}   vol×: {vm}\n24h vol: {v24s}\n{ts}"
""".lstrip("\n")
    # вставим перед первой функцией или в конец
    m = re.search(r"\n\s*def\s+\w+\s*\(", s)
    if m:
        insert_at = m.start()
        return s[:insert_at] + helper + s[insert_at:]
    else:
        return s + ("\n\n" + helper)

def hook_after_signal_logs(s: str) -> str:
    """
    Находит места, где есть logging.info("[signal] ...", sig) или эквивалент,
    и вставляет однотипный send-блок сразу после.
    """
    # шаблоны возможных форматов логирования
    patterns = [
        r'logging\.info\(\s*["\']\[signal\][^)]*sig[^)]*\)\s*',
        r'logger\.info\(\s*["\']\[signal\][^)]*sig[^)]*\)\s*',
        r'print\(\s*["\']\[signal\][^)]*sig[^)]*\)\s*',
    ]
    out = s
    for pat in patterns:
        def repl(m):
            block = m.group(0)
            # если сразу после уже вставлен наш send-блок — не дублируем
            after = out[m.end():m.end()+300]
            if "TG.send_message(_fmt_signal(sig))" in after:
                return block
            return block + "\n" + SEND_BLOCK + "\n"
        out = re.sub(pat, repl, out)
    return out

def main():
    ts = time.strftime("%Y%m%d_%H%M%S")
    src = read_text(MAIN)

    bak = MAIN.with_suffix(f".py.tgpatch.bak.{ts}")
    write_text(bak, src)

    s = src
    s = ensure_future_import(s)
    s = ensure_imports(s)
    s = ensure_tg_init(s)
    s = ensure_fmt_helper(s)
    s = hook_after_signal_logs(s)

    if s != src:
        write_text(MAIN, s)
        print(f"Patched main.py -> {MAIN}")
        print(f"Backup saved to {bak}")
    else:
        print("main.py already has TG integration; nothing to change.")

if __name__ == "__main__":
    main()
