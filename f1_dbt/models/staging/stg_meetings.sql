select
    meeting_key, meeting_name, meeting_official_name, location,
    country_code, country_name, circuit_key, circuit_short_name,
    circuit_type,
    cast(date_start as timestamp_ntz) as date_start,
    cast(date_end as timestamp_ntz) as date_end,
    year, is_cancelled
from {{ source('raw', 'MEETINGS') }}