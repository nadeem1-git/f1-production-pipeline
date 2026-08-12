# Each entity: topic name, OpenF1 endpoint, and the field to use as Kafka key
ENTITIES = [
    {"topic": "meetings",     "endpoint": "meetings",     "key_field": "meeting_key"},
    {"topic": "drivers",      "endpoint": "drivers",      "key_field": "driver_number"},
    {"topic": "laps",         "endpoint": "laps",         "key_field": "session_key"},
    {"topic": "positions",    "endpoint": "position",     "key_field": "session_key"},
    {"topic": "pit_stops",    "endpoint": "pit",          "key_field": "session_key"},
    {"topic": "stints",       "endpoint": "stints",       "key_field": "session_key"},
    {"topic": "weather",      "endpoint": "weather",      "key_field": "session_key"},
    {"topic": "race_control", "endpoint": "race_control", "key_field": "session_key"},
    {"topic": "team_radio",   "endpoint": "team_radio",   "key_field": "session_key"},
]