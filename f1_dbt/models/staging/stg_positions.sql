select
    session_key, meeting_key, driver_number, position,
    cast(date as timestamp_ntz) as position_time
from {{ source('raw', 'POSITIONS') }}