select
    meeting_key, session_key, driver_number,
    cast(date as timestamp_ntz) as radio_time,
    recording_url
from {{ source('raw', 'TEAM_RADIO') }}