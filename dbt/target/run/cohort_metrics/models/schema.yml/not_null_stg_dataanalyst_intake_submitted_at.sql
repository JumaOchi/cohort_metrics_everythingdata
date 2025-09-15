
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select submitted_at
from "postgres"."analytics"."stg_dataanalyst_intake"
where submitted_at is null



  
  
      
    ) dbt_internal_test