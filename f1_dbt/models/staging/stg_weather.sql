select
    session_key, meeting_key,
    cast(date as timestamp_ntz) as weather_time,
    rainfall, humidity, air_temperature, track_temperature,
    pressure, wind_direction, wind_speed
from {{ source('raw', 'WEATHER') }}