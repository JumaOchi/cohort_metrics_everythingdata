{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_datascience_intake') }}
    union all
    select * from {{ ref('stg_dataanalyst_intake') }}
)

select
    id_no,
    track,
    total_score,
    graduated,
    case when graduated = true then 1 else 0 end as graduation_flag
from base
