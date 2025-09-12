
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_no
from "postgres"."analytics"."fact_application_details"
where id_no is null



  
  
      
    ) dbt_internal_test