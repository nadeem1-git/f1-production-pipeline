select
    driver_number,
    full_name,
    name_acronym,
    team_name,
    team_colour,
    country_code
from {{ ref('stg_drivers') }}
qualify row_number() over (partition by driver_number order by session_key desc) = 1