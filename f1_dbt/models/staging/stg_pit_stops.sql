select
    session_key, meeting_key, driver_number, lap_number,
    lane_duration, stop_duration, pit_duration,
    cast(date as timestamp_ntz) as pit_time
from {{ source('raw', 'PIT_STOPS') }}