import requests
from kafka_utils import get_producer

OPENF1_BASE = "https://api.openf1.org/v1"
TOPIC = "sessions"

def fetch_sessions(year: int):
    url = f"{OPENF1_BASE}/sessions?year={year}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def main():
    producer = get_producer()
    total = 0

    for year in [2023, 2024, 2025]:
        sessions = fetch_sessions(year)
        for s in sessions:
            producer.send(TOPIC, key=s["session_key"], value=s)
            total += 1
        print(f"Year {year}: {len(sessions)} sessions published")

    producer.flush()
    print(f"Done. {total} sessions published to '{TOPIC}' topic.")

if __name__ == "__main__":
    main()