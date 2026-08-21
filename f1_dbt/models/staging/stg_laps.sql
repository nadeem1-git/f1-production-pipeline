select
    meeting_key, session_key, driver_number, lap_number,
    cast(date_start as timestamp_ntz) as date_start,
    duration_sector_1, duration_sector_2, duration_sector_3,
    i1_speed, i2_speed, is_pit_out_lap, lap_duration, st_speed
from {{ source('raw', 'LAPS') }}