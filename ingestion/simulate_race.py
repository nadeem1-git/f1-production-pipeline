import time
import random
import requests
from datetime import datetime
from kafka_utils import get_producer

OPENF1_BASE = "https://api.openf1.org/v1"
SESSION_KEY = 9662              # 2024 Abu Dhabi GP - Race
SPEED_MULTIPLIER = 60           # 60x real-time: ~2hr race replays in ~2 min
LATE_EVENT_RATE = 0.03          # 3% of events arrive deliberately late
DUPLICATE_EVENT_RATE = 0.02     # 2% of events sent twice

# endpoint -> (kafka topic, timestamp field, key field)
TIMED_ENTITIES = {
    "laps":         ("laps", "date_start", "driver_number"),
    "position":     ("positions", "date", "driver_number"),
    "pit":          ("pit_stops", "date", "driver_number"),
    "weather":      ("weather", "date", "session_key"),
    "race_control": ("race_control", "date", "driver_number"),
    "team_radio":   ("team_radio", "date", "driver_number"),
}

def fetch(endpoint):
    r = requests.get(f"{OPENF1_BASE}/{endpoint}", params={"session_key": SESSION_KEY})
    r.raise_for_status()
    return r.json()

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def build_event_stream():
    events = []
    for endpoint, (topic, ts_field, key_field) in TIMED_ENTITIES.items():
        for r in fetch(endpoint):
            if not r.get(ts_field):
                continue
            events.append({
                "topic": topic,
                "key": r.get(key_field, SESSION_KEY),
                "value": r,
                "ts": parse_ts(r[ts_field]),
            })
    events.sort(key=lambda e: e["ts"])
    return events

def publish_reference_data(producer):
    stints = fetch("stints")
    for s in stints:
        producer.send("stints", key=s.get("driver_number"), value=s)
    producer.flush()
    print(f"stints: {len(stints)} records published (unpaced reference data)")

def main():
    producer = get_producer()

    print("Publishing reference data (stints)...")
    publish_reference_data(producer)

    print("Building timed event stream...")
    events = build_event_stream()
    print(f"{len(events)} timed events to replay at {SPEED_MULTIPLIER}x speed")

    late_queue = []
    prev_ts = None
    sent = duplicated = delayed = 0

    for event in events:
        if prev_ts is not None:
            real_gap = (event["ts"] - prev_ts).total_seconds()
            time.sleep(max(real_gap / SPEED_MULTIPLIER, 0))
        prev_ts = event["ts"]

        if random.random() < LATE_EVENT_RATE:
            late_queue.append(event)
            delayed += 1
            continue

        producer.send(event["topic"], key=event["key"], value=event["value"])
        sent += 1

        if random.random() < DUPLICATE_EVENT_RATE:
            producer.send(event["topic"], key=event["key"], value=event["value"])
            duplicated += 1

        if late_queue and random.random() < 0.3:
            late_event = late_queue.pop(0)
            producer.send(late_event["topic"], key=late_event["key"], value=late_event["value"])
            sent += 1

    for late_event in late_queue:
        producer.send(late_event["topic"], key=late_event["key"], value=late_event["value"])
        sent += 1

    producer.flush()
    print(f"Simulation complete. {sent} sent, {duplicated} duplicated, {delayed} deliberately delayed.")

if __name__ == "__main__":
    main()