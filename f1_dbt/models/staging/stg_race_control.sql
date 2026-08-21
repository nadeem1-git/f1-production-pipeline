select
    meeting_key, session_key, driver_number, lap_number,
    cast(date as timestamp_ntz) as event_time,
    category, flag, scope, sector, qualifying_phase, message
from {{ source('raw', 'RACE_CONTROL') }}