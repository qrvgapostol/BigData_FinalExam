"""
Kafka Producer for Streaming Data Dashboard
Real API: Binance BTC price in USDT
"""

import argparse
import json
import time
from datetime import datetime

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        try:
            servers = [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
            self.producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Producer connected to Kafka at {self.bootstrap_servers}")
        except NoBrokersAvailable:
            print(f"No Kafka brokers available at {self.bootstrap_servers}")
            self.producer = None

    def fetch_api_data(self):
        """Fetch BTC price from Binance and build the project schema."""
        try:
            resp = requests.get(BINANCE_URL, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            price = float(data["price"])

            return {
                "timestamp": datetime.utcnow().isoformat(),
                "value": price,
                "metric_type": "BTC_USD",
                "sensor_id": "binance_btc",
                "source": "api",
            }
        except Exception as e:
            print("API error:", e)
            return None

    def serialize_data(self, data):
        try:
            return json.dumps(data).encode("utf-8")
        except Exception as e:
            print("Serialize error:", e)
            return None

    def send_message(self, data):
        if not self.producer:
            return False
        serialized = self.serialize_data(data)
        if not serialized:
            return False
        try:
            self.producer.send(self.topic, value=data)
            self.producer.flush()
            return True
        except KafkaError as e:
            print("Kafka send error:", e)
            return False

    def produce_stream(self, messages_per_second=1 / 15.0):
        """Send one message about every 15 seconds."""
        delay = 1 / messages_per_second
        print("Producer running. Ctrl+C to stop.")
        try:
            while True:
                msg = self.fetch_api_data()
                if msg:
                    ok = self.send_message(msg)
                    if ok:
                        print("Sent:", msg)
                time.sleep(delay)
        except KeyboardInterrupt:
            print("Producer stopped.")

    def close(self):
        if self.producer:
            self.producer.close()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9093")
    parser.add_argument("--topic", default="streaming-data")
    parser.add_argument("--rate", type=float, default=1 / 15.0)
    return parser.parse_args()


def main():
    args = parse_arguments()
    prod = StreamingDataProducer(args.bootstrap_servers, args.topic)
    prod.produce_stream(messages_per_second=args.rate)


if __name__ == "__main__":
    main()
