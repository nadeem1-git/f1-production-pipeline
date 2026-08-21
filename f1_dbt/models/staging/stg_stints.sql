select
    meeting_key, session_key, driver_number, stint_number,
    lap_start, lap_end, compound, tyre_age_at_start
from {{ source('raw', 'STINTS') }}