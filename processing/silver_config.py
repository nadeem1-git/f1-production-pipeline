# entity -> columns that must not be null for a record to be considered valid
SILVER_CONFIG = {
    "laps":         {"required": ["session_key", "driver_number", "lap_number", "date_start"]},
    "sessions": {"required": ["session_key", "session_type"]},
    "meetings":     {"required": ["meeting_key"]},
    "drivers":      {"required": ["session_key", "driver_number"]},
    "positions":    {"required": ["session_key", "driver_number", "date"]},
    "pit_stops":    {"required": ["session_key", "driver_number", "lap_number", "date"]},
    "stints":       {"required": ["session_key", "driver_number", "stint_number"]},
    "weather":      {"required": ["session_key", "date"]},
    "race_control": {"required": ["session_key", "date"]},
    "team_radio":   {"required": ["session_key", "driver_number", "date"]},
}