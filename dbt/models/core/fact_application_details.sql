{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_datascience_intake') }}
    union all
    select * from {{ ref('stg_dataanalyst_intake') }}
)

select
    id_no,
    track,
    referral_source,
    weekly_commitment,
    main_aim,
    motivation,
    skill_level,
    aptitude_test_completed
from base
