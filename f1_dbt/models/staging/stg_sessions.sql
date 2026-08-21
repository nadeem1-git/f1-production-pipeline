select
    session_key, session_type, session_name,
    cast(date_start as timestamp_ntz) as date_start,
    cast(date_end as timestamp_ntz) as date_end,
    meeting_key, circuit_key, circuit_short_name,
    country_code, country_name, location, year, is_cancelled
from {{ source('raw', 'SESSIONS') }}