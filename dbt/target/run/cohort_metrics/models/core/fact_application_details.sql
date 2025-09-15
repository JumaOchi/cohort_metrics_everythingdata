
  
    

  create  table "postgres"."analytics"."fact_application_details__dbt_tmp"
  
  
    as
  
  (
    

with base as (
    select * from "postgres"."analytics"."stg_datascience_intake"
    union all
    select * from "postgres"."analytics"."stg_dataanalyst_intake"
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
  );
  