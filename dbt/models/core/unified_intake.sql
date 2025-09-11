{{ config(materialized='table') }}

with ds as (
  select *, 'Data Science' as track_type
  from {{ ref('stg_datascience_intake') }}
),

da as (
  select *, 'Data Analyst' as track_type
  from {{ ref('stg_dataanalyst_intake') }}
)

select
  id_no,
  submitted_at,
  age_range,
  gender,
  country,
  referral_source,
  years_experience,
  track,
  weekly_commitment,
  main_aim,
  motivation,
  skill_level,
  aptitude_test_completed,
  total_score,
  graduated,
  track_type
from ds

union all

select
  id_no,
  submitted_at,
  age_range,
  gender,
  country,
  referral_source,
  years_experience,
  track,
  weekly_commitment,
  main_aim,
  motivation,
  skill_level,
  aptitude_test_completed,
  total_score,
  graduated,
  track_type
from da
