"""
Export Kafka topic messages to a CSV file (Excel-friendly table).

Usage:
    python export_to_csv.py                          # exports 'container-telemetry' topic
    python export_to_csv.py --topic my-topic          # export a different topic
    python export_to_csv.py --out my_export.csv       # choose output filename

Requires:
    pip install kafka-python
"""

import argparse
import csv
import json

from kafka import KafkaConsumer

CSV_FIELDS = [
    "Timestamp", "Container_ID", "Cargo_Type", "Origin", "Destination",
    "Temperature_C", "Humidity_Percent", "Vibration_Level",
    "Distance_Remaining_km", "Spoilage_Risk", "Recommended_Action", "Alert_Status",
]


def main():
    parser = argparse.ArgumentParser(description="Export Kafka topic messages to CSV")
    parser.add_argument("--topic", type=str, default="container-telemetry")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092")
    parser.add_argument("--out", type=str, default="telemetry_export.csv")
    parser.add_argument("--timeout-ms", type=int, default=5000,
                         help="Stop after this many ms with no new messages")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest",
        consumer_timeout_ms=args.timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    count = 0
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for message in consumer:
            writer.writerow(message.value)
            count += 1

    print(f"Exported {count} messages to {args.out}")


if __name__ == "__main__":
    main()
