"""
AtmoSync - Real-Time IoT Telemetry Simulator
-----------------------------------------------
Generates continuous, realistic fake sensor telemetry for containers shipping
agricultural cargo from Bengaluru to Delhi, matching the required schema:

    Timestamp, Container_ID, Cargo_Type, Origin, Destination,
    Temperature_C, Humidity_Percent, Vibration_Level,
    Distance_Remaining_km, Spoilage_Risk, Recommended_Action, Alert_Status

Spoilage_Risk, Recommended_Action, and Alert_Status are NOT random — they are
derived from actual sensor readings (how far temperature has drifted from the
cargo's ideal range, for how long, plus vibration spikes), so the data behaves
like a real monitoring system rather than random labels.

Usage:
    python iot_telemetry_simulator.py                      # print to console, forever
    python iot_telemetry_simulator.py --out data.jsonl      # also append to a file (JSON Lines)
    python iot_telemetry_simulator.py --csv data.csv        # also append to a CSV file (Excel-friendly table)
    python iot_telemetry_simulator.py --interval 2          # change reading frequency (sec)
    python iot_telemetry_simulator.py --kafka                # ALSO stream into Kafka topic
    python iot_telemetry_simulator.py --containers 1 --kafka

Requires (for --kafka mode):
    pip install kafka-python
    Kafka must be running locally on localhost:9092 (see docker-compose.yml)
"""

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


# Fixed route for this simulation: Bengaluru -> Delhi (approx. road distance)
ORIGIN = "Bengaluru"
DESTINATION = "Delhi"
ROUTE_DISTANCE_KM = 2150

# Ideal storage temperature range (deg C) per cargo type — real-world cold-chain
# reference values. Spoilage risk is driven by how far the actual temperature
# strays outside this range, and for how long.
IDEAL_TEMP_RANGE = {
    "Avocado":    (5.0, 8.0),
    "Banana":     (13.0, 15.0),
    "Mango":      (10.0, 13.0),
    "Grapes":     (0.0, 2.0),
    "Strawberry": (0.0, 2.0),
}

CARGO_TYPES = list(IDEAL_TEMP_RANGE.keys())


class ContainerSensor:
    """Simulates one container's sensor cluster and derived risk state."""

    def __init__(self, container_id, cargo_type, is_drifting=False):
        self.container_id = container_id
        self.cargo_type = cargo_type
        self.is_drifting = is_drifting

        low, high = IDEAL_TEMP_RANGE[cargo_type]
        mid = (low + high) / 2

        # Start near the ideal midpoint for this cargo
        self.temperature = round(random.uniform(low, high), 2)
        self.humidity = round(random.uniform(85.0, 90.0), 2)
        self.vibration = round(random.uniform(0.01, 0.05), 3)

        # How fast a drifting container's temperature creeps out of range
        self.drift_rate = round(random.uniform(0.03, 0.08), 3)

        # Journey simulation
        self.distance_remaining_km = ROUTE_DISTANCE_KM
        self.speed_kmph = random.uniform(45, 65)

        # Cumulative "stress" score — rises while temperature is out of the
        # ideal range (further out of range = rises faster), decays slowly
        # when back in range. This is what actually drives Spoilage_Risk.
        self.stress_score = 0.0

    def _random_walk(self, value, step, low, high):
        value += random.uniform(-step, step)
        return round(max(low, min(high, value)), 3)

    def _advance_journey(self, interval_seconds):
        km_this_tick = self.speed_kmph * (interval_seconds / 3600)
        self.distance_remaining_km = max(0.0, round(self.distance_remaining_km - km_this_tick, 2))

    def _update_stress(self):
        low, high = IDEAL_TEMP_RANGE[self.cargo_type]
        if self.temperature < low:
            deviation = low - self.temperature
        elif self.temperature > high:
            deviation = self.temperature - high
        else:
            deviation = 0.0

        if deviation > 0:
            self.stress_score += deviation * 1.5
        else:
            self.stress_score = max(0.0, self.stress_score * 0.95)  # slow decay

        # Vibration spikes add a bit of stress too (rough handling risk)
        if self.vibration > 0.5:
            self.stress_score += 2.0

    def _derive_status(self):
        if self.stress_score < 8:
            risk = "Low"
        elif self.stress_score < 20:
            risk = "Medium"
        else:
            risk = "High"

        action = "Reroute" if risk == "High" else "Continue"

        if risk == "High" or self.vibration > 0.8:
            alert = "Critical"
        elif risk == "Medium" or self.vibration > 0.5:
            alert = "Warning"
        else:
            alert = "Normal"

        return risk, action, alert

    def read(self, interval_seconds=1.0):
        # Sensor fluctuation
        self.humidity = self._random_walk(self.humidity, 0.4, 70, 95)
        self.vibration = self._random_walk(self.vibration, 0.02, 0.0, 0.6)

        if self.is_drifting:
            # Simulates a failing cooling unit -> temperature creeps out of range
            self.temperature += self.drift_rate + random.uniform(-0.01, 0.02)
            self.temperature = round(self.temperature, 3)
            if random.random() < 0.05:
                self.vibration = round(self.vibration + random.uniform(0.3, 0.8), 3)
        else:
            low, high = IDEAL_TEMP_RANGE[self.cargo_type]
            self.temperature = self._random_walk(self.temperature, 0.15, low - 1, high + 1)

        self._advance_journey(interval_seconds)
        self._update_stress()
        risk, action, alert = self._derive_status()

        return {
            "Timestamp": datetime.now(timezone.utc).isoformat(),
            "Container_ID": self.container_id,
            "Cargo_Type": self.cargo_type,
            "Origin": ORIGIN,
            "Destination": DESTINATION,
            "Temperature_C": round(self.temperature, 2),
            "Humidity_Percent": round(self.humidity, 2),
            "Vibration_Level": round(self.vibration, 3),
            "Distance_Remaining_km": self.distance_remaining_km,
            "Spoilage_Risk": risk,
            "Recommended_Action": action,
            "Alert_Status": alert,
        }


def build_fleet(num_containers=5):
    """Creates a fleet of containers. One is randomly chosen to 'drift' (spoilage risk).

    Cargo assignment guarantees every cargo type in CARGO_TYPES appears at least
    once (as long as num_containers >= number of cargo types) instead of relying
    on pure random.choice, which can easily skip some types on small fleets.
    """
    drifting_index = random.randint(0, num_containers - 1)

    # Build a list of cargo assignments: one guaranteed pass through every
    # cargo type first (shuffled), then fill any remaining slots randomly.
    cargo_assignments = []
    shuffled_types = CARGO_TYPES.copy()
    random.shuffle(shuffled_types)
    for i in range(num_containers):
        if i < len(shuffled_types):
            cargo_assignments.append(shuffled_types[i])
        else:
            cargo_assignments.append(random.choice(CARGO_TYPES))
    random.shuffle(cargo_assignments)  # so it's not always CONT-1000 = first type

    fleet = []
    for i in range(num_containers):
        container_id = f"CONT-{1000 + i}"
        cargo = cargo_assignments[i]
        fleet.append(
            ContainerSensor(
                container_id=container_id,
                cargo_type=cargo,
                is_drifting=(i == drifting_index),
            )
        )
    return fleet


CSV_FIELDS = [
    "Timestamp", "Container_ID", "Cargo_Type", "Origin", "Destination",
    "Temperature_C", "Humidity_Percent", "Vibration_Level",
    "Distance_Remaining_km", "Spoilage_Risk", "Recommended_Action", "Alert_Status",
]


def emit(reading, out_file=None, csv_file=None, producer=None, topic="container-telemetry"):
    """
    Single place where each reading is 'sent' somewhere:
    - always prints to console
    - optionally appends to a .jsonl file
    - optionally appends to a .csv file (Excel-friendly table, header written once)
    - optionally sends to a Kafka topic (if a producer is passed in)
    """
    line = json.dumps(reading)
    print(line)

    if out_file:
        with open(out_file, "a") as f:
            f.write(line + "\n")

    if csv_file:
        file_is_new = not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
        with open(csv_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if file_is_new:
                writer.writeheader()
            writer.writerow(reading)

    if producer:
        producer.send(topic, value=reading)


def build_kafka_producer(bootstrap_servers="localhost:9092"):
    if not KAFKA_AVAILABLE:
        raise RuntimeError("kafka-python is not installed. Run: pip install kafka-python")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main():
    parser = argparse.ArgumentParser(description="AtmoSync real-time IoT telemetry simulator")
    parser.add_argument("--containers", type=int, default=5, help="Number of containers to simulate")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between readings")
    parser.add_argument("--out", type=str, default=None, help="Optional .jsonl file to append readings to")
    parser.add_argument("--csv", type=str, default=None, help="Optional .csv file to append readings to (opens as a table in Excel)")
    parser.add_argument("--kafka", action="store_true", help="Also stream readings into a Kafka topic")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", type=str, default="container-telemetry", help="Kafka topic name")
    args = parser.parse_args()

    fleet = build_fleet(args.containers)
    print(f"# Simulating {len(fleet)} containers on route {ORIGIN} -> {DESTINATION} "
          f"({ROUTE_DISTANCE_KM} km). Press Ctrl+C to stop.\n")

    producer = None
    if args.kafka:
        print(f"# Connecting to Kafka at {args.bootstrap_server} ...")
        producer = build_kafka_producer(args.bootstrap_server)
        print(f"# Connected. Streaming into topic '{args.topic}'.\n")

    try:
        while True:
            for sensor in fleet:
                reading = sensor.read(interval_seconds=args.interval)
                emit(reading, args.out, csv_file=args.csv, producer=producer, topic=args.topic)
            time.sleep(args.interval)
            if producer:
                producer.flush()
    except KeyboardInterrupt:
        print("\n# Simulation stopped.")
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
