

with unified as (
    select
        id_no,
        age_range,
        gender,
        country,
        track as track_type
    from "postgres"."analytics"."stg_datascience_intake"
    union all
    select
        id_no,
        age_range,
        gender,
        country,
        track as track_type
    from "postgres"."analytics"."stg_dataanalyst_intake"
)

select
    id_no,
    age_range,
    gender,
    country,
    track_type
from unified