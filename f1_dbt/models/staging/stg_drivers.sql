select
    meeting_key, session_key, driver_number, broadcast_name,
    full_name, name_acronym, team_name, team_colour,
    first_name, last_name, country_code
from {{ source('raw', 'DRIVERS') }}