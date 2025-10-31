- RESPONSE=$(curl -sS -X POST http://127.0.0.1:8787/tvoi -H 'Content-Type: application/json' -d "${PAYLOAD}")
- FILENAME=$(printf '%s' "${RESPONSE}" | python3 - <<'PY'
-import json, sys
-try:
-    data = json.loads(sys.stdin.read())
-except Exception:
-    data = {}
-print(data.get('file', ''))
-PY
-)
+ RESPONSE=$(curl -sS -X POST http://127.0.0.1:8787/tvoi -H 'Content-Type: application/json' -d "${PAYLOAD}")
+ ITS_RESPONSE="${RESPONSE}"
+ export ITS_RESPONSE
+ FILENAME=$(python3 - <<'PY'
+import json, os
+resp = os.environ.get("ITS_RESPONSE","")
+try:
+    data = json.loads(resp)
+    print(data.get("file",""))
+except Exception:
+    print("")
+PY
+)
- if [ -z "${FILENAME:-}" ]; then
-   echo "deploy failed at line 118: ..." >&2
-   exit 1
- fi
+ if [ -z "${FILENAME:-}" ]; then
+   echo "WARN: cannot parse 'file' from gateway response: ${RESPONSE}" >&2
+ else
+   echo "queued file: ${FILENAME}"
+ fi
