# Source Design Card (checkpoint A)

**Name:** Musayab Razaq
**Date:** Mon 16 Nov 2026 (catching up on Session 1)

## 1. The source
- Source and URL: Coinbase Exchange WebSocket feed, `wss://ws-feed.exchange.coinbase.com`, channel `matches`, 6 products (BTC-USD, ETH-USD, SOL-USD, XRP-USD, DOGE-USD, ADA-USD)
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: no account or key; the producer sends a User-Agent
- Limits I know about (rate limits, paging, reconnects): the subscribe message must arrive within 5 s of connecting or Coinbase closes the connection; Coinbase warns that messages on this channel can be dropped; no replay, so anything missed while offline is gone. The producer reconnects after a 5 s pause.

## 2. One real message
The first messages are not trades: one `subscriptions` reply, then one `last_match` per product. A live trade has the same fields with `"type": "match"`.

```json
{
  "type": "last_match",
  "trade_id": 1132009328,
  "maker_order_id": "c777abb2-7d36-415c-8326-4d16b6fb4860",
  "taker_order_id": "e1a8bfe1-3da1-42ac-98e5-473c2713e752",
  "side": "buy",
  "size": "0.00000011",
  "price": "64522.48",
  "product_id": "BTC-USD",
  "sequence": 139574460800,
  "time": "2026-11-16T15:53:06.439568Z"
}
```

## 3. Volume maths
- Measured rate (from the probe): 35.47 per second (60 s probe, 2,132 messages, Mon 16 Nov 16:53 Valencia time)
- Live capture needed for 50,000: about 23 minutes at that rate
- History / backfill available? How many days? None: the WebSocket has no replay
- My plan to reach 50,000 raw records: producer running live since 17:12; 28,951 messages in the topic at 17:24; keep it running past 60,000 for margin, then land everything with the lander

## 4. Topic design
- Topic name: `coinbase_trades` (created with `-p 3 -r 3` before the producer ran)
- Kafka key: `product_id`
- Why this key (ordering, spread across partitions): every trade of one product goes to the same partition, so trades of a product stay in order, which matters for per-product questions and for spotting `sequence` gaps. The cost is uneven partitions: at 17:24 partition 0 had 20,384, partition 1 had 1,625 and partition 2 had 6,942, because the busiest products hash to partition 0.

## 5. Duplicates
- What makes two records **the same Kafka record**? The same `kafka_partition` + `kafka_offset`: the lander can land a record twice after a crash before offsets are stored (at-least-once).
- What makes two records **the same thing**? The same `product_id` + `trade_id`. A `last_match` after a reconnect can repeat a trade that already arrived as a `match`.
- If the same thing can arrive in several versions, which one do I keep? A trade does not change after it happens, so every copy is identical: keep one (the highest offset).

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: WRITE YOURS HERE
When BTC-USD bursts (a 1-minute trade count above 3× its 10-minute average), do the other coins burst in the same minute?

## 7. Self-check (checkpoint A)
- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [ ] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join