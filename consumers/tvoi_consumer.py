@@
-from telegram import Bot
+from telegram import Bot
+import asyncio
@@
-APP = Path("/home/deploy/apps/innertrade-screener")
-INBOX = APP / "inbox"
-PROCESSED = APP / "processed"
-FAILED = APP / "failed"
+APP = Path("/home/deploy/apps/innertrade-screener")
+INBOX = APP / "inbox"
+PROCESSED = APP / "processed"
+FAILED = APP / "failed"
+for d in (INBOX, PROCESSED, FAILED):
+    d.mkdir(parents=True, exist_ok=True)
@@
-load_dotenv(dotenv_path=APP / ".env")
-BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
-# CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # ID или @channelusername
-# ... здесь твоя логика чтения файла и bot.send_message(...)
+load_dotenv(dotenv_path=APP / ".env")
+BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
+CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
+if not BOT_TOKEN or not CHAT_ID:
+    print("ERR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы в .env", file=sys.stderr)
+    sys.exit(2)
+bot = Bot(token=BOT_TOKEN)
+
+def send_tg(text: str) -> None:
+    # PTB v20+: send_message — async
+    asyncio.run(bot.send_message(chat_id=CHAT_ID, text=text, disable_web_page_preview=True))
+
+def main_loop():
+    while True:
+        for p in sorted(INBOX.glob("*.json")):
+            try:
+                data = json.loads(p.read_text(encoding="utf-8"))
+                text = f"TVOI PRE | {data.get('symbol')} {data.get('tf')} | price={data.get('price')} | {data.get('link')}"
+                send_tg(text)
+                shutil.move(str(p), PROCESSED / p.name)
+                print(f"{datetime.utcnow().isoformat()}Z | TVOI PRE sent=True | file={p.name}")
+            except Exception as e:
+                print(f"{datetime.utcnow().isoformat()}Z | TVOI PRE sent=False | file={p.name} | err={e}", file=sys.stderr)
+                try:
+                    shutil.move(str(p), FAILED / p.name)
+                except Exception:
+                    pass
+        time.sleep(1.0)
+
+if __name__ == "__main__":
+    print(f"{datetime.utcnow().isoformat()}Z | TVOI consumer start | inbox={INBOX}")
+    main_loop()
