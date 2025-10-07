# ScreenerBot (Signals -> Telegram)

Форвардер сигналов из движка в Telegram по жёстким правилам отбора.

## Потоки

- Движок (на VPS) отдаёт `GET /signals` со списком объектов:
  ```json
  {
    "data": [
      {
        "ts": "2025-10-07 05:00:00",
        "symbol": "BTCUSDT",
        "close": 61234.5,
        "zprice": 2.31,
        "vol_mult": 2.05,
        "vol24h_usd": 123456789.0,
        "bar_ts": 1759812000000,
        "oi_z": 0.92        // может отсутствовать
      }
    ]
  }
