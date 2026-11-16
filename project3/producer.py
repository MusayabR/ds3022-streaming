"""Coinbase trades -> Kafka topic "coinbase_trades".

  python producer.py     # PUSH: live trades ("matches") from the Coinbase Exchange WebSocket

Adapted from quakes_producer.py (Mon 02 Nov). The WebSocket connection and the
reconnect loop follow probe.py and the Session 1 worksheet (Appendix A).
"""
import asyncio
import json
import logging

from quixstreams import Application
from tornado.httpclient import HTTPRequest
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"
WSS_URL = "wss://ws-feed.exchange.coinbase.com"
USER_AGENT = "DS3022-Project3-producer/1.0 (UVA in Valencia student project)"
SUBSCRIBE = {"type": "subscribe",
             "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"],
             "channels": ["matches"]}
LOG_EVERY = 1000


def send(producer, message):
    """One Kafka record per message. Key = product_id, so every trade of one
    product goes to the same partition, in order."""
    key = message["data"].get("product_id")  # .get(): no crash if it is missing
    if key is None:
        logging.warning("No product_id (type=%s): sent without a key", message["data"].get("type"))
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))


async def live(producer):
    """PUSH over a WebSocket. If the connection drops, wait 5 s and reconnect."""
    sent = 0
    while True:
        try:
            request = HTTPRequest(WSS_URL, headers={"User-Agent": USER_AGENT})
            ws = await websocket_connect(request, ping_interval=15)
            await ws.write_message(json.dumps(SUBSCRIBE))  # within 5 s, or Coinbase closes it
            logging.info("Connected and subscribed. Waiting for trades...")
            while True:
                text = await ws.read_message()
                if text is None:
                    logging.warning("The server closed the connection")
                    break
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    logging.warning("Skipped a message I could not read: %r", e)
                    continue
                sent += 1
                if sent % LOG_EVERY == 0:
                    logging.info("%d messages sent to %s", sent, TOPIC)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Stopped")