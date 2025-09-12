
  
    

  create  table "postgres"."analytics"."mart_total_intake_summary__dbt_tmp"
  
  
    as
  
  (
    

select
  track_type,
  country,
  age_range,
  count(*) as total_applicants,
  avg(total_score) as avg_score,
  sum(case when aptitude_test_completed then 1 else 0 end)::float / nullif(count(*),0) as completion_rate,
  sum(case when graduated then 1 else 0 end)::float / nullif(count(*),0) as graduation_rate
from "postgres"."analytics"."unified_intake"
group by 1,2,3
  );
  