from great_expectations import expectations as gxe

QUALITY_CHECKS = {
    "laps": {
        "table": "local.silver.laps",
        "pre_filter": "is_pit_out_lap = false",
        "checks": [
            gxe.ExpectColumnValuesToBeBetween(column="lap_duration", min_value=0, max_value=600),
            gxe.ExpectColumnValuesToBeBetween(column="duration_sector_1", min_value=0, max_value=300),
            gxe.ExpectColumnValuesToBeBetween(column="duration_sector_2", min_value=0, max_value=300),
            gxe.ExpectColumnValuesToBeBetween(column="duration_sector_3", min_value=0, max_value=300),
        ],
    },
    "positions": {
        "table": "local.silver.positions",
        "checks": [
            gxe.ExpectColumnValuesToBeBetween(column="position", min_value=1, max_value=30),
        ],
    },
    "pit_stops": {
        "table": "local.silver.pit_stops",
        "pre_filter": "stop_duration IS NOT NULL",
        "checks": [
            gxe.ExpectColumnValuesToBeBetween(column="pit_duration", min_value=0, max_value=300),
            gxe.ExpectColumnValuesToBeBetween(column="stop_duration", min_value=0, max_value=120),
        ],
    },
    "weather": {
        "table": "local.silver.weather",
        "checks": [
            gxe.ExpectColumnValuesToBeBetween(column="air_temperature", min_value=-10, max_value=60),
            gxe.ExpectColumnValuesToBeBetween(column="track_temperature", min_value=-10, max_value=80),
            gxe.ExpectColumnValuesToBeBetween(column="humidity", min_value=0, max_value=100),
        ],
    },
    "drivers": {
        "table": "local.silver.drivers",
        "checks": [
            gxe.ExpectColumnValuesToBeBetween(column="driver_number", min_value=1, max_value=99),
        ],
    },
}