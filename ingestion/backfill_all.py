import time
import requests
from kafka_utils import get_producer
from entities_config import ENTITIES

OPENF1_BASE = "https://api.openf1.org/v1"
YEARS = [2023, 2024, 2025]
TEST_MODE = False         # keep True for now — limits to a few sessions
TEST_SESSION_LIMIT = 3

SLEEP_SECONDS = 2.5   # stays safely under OpenF1's 30 req/min cap
MAX_RETRIES = 4

def fetch(endpoint, params=None):
    url = f"{OPENF1_BASE}/{endpoint}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5)) * (attempt + 1)
                print(f"  [rate-limited] {endpoint} {params} -> waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            print(f"  [warn] {endpoint} failed for {params}: {e}")
            return []
    print(f"  [error] {endpoint} {params} -> gave up after {MAX_RETRIES} retries")
    return []

def get_session_keys():
    sessions = []
    for year in YEARS:
        sessions.extend(fetch("sessions", {"year": year}))
    keys = [s["session_key"] for s in sessions]
    if TEST_MODE:
        keys = keys[:TEST_SESSION_LIMIT]
    return keys

def main():
    producer = get_producer()
    session_keys = get_session_keys()
    print(f"Backfilling {len(session_keys)} session(s) {'(TEST MODE)' if TEST_MODE else ''}")

    for entity in ENTITIES:
        topic, endpoint, key_field = entity["topic"], entity["endpoint"], entity["key_field"]
        total = 0

        if endpoint == "meetings":
            for year in YEARS:
                for r in fetch(endpoint, {"year": year}):
                    producer.send(topic, key=r.get(key_field), value=r)
                    total += 1
                time.sleep(SLEEP_SECONDS)
        else:
            for session_key in session_keys:
                for r in fetch(endpoint, {"session_key": session_key}):
                    producer.send(topic, key=r.get(key_field, session_key), value=r)
                    total += 1
                time.sleep(SLEEP_SECONDS)

        producer.flush()
        print(f"{topic}: {total} records published")

    print("Backfill run complete.")

if __name__ == "__main__":
    main()