select
    session_key,
    driver_number,
    lap_number,
    pit_time,
    lane_duration,
    stop_duration,
    pit_duration
from {{ ref('stg_pit_stops') }}