
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    id_no as unique_field,
    count(*) as n_records

from "postgres"."analytics"."unified_intake"
where id_no is not null
group by id_no
having count(*) > 1



  
  
      
    ) dbt_internal_test