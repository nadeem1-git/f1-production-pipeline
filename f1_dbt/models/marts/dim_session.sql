select
    s.session_key,
    s.session_type,
    s.session_name,
    s.date_start,
    s.date_end,
    s.meeting_key,
    m.meeting_name,
    s.circuit_short_name,
    s.country_name,
    s.location,
    s.year
from {{ ref('stg_sessions') }} s
left join {{ ref('stg_meetings') }} m on s.meeting_key = m.meeting_key