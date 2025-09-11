{{ config(materialized='view') }}

with raw as (
  select
    id_no,
    "timestamp" as submitted_at,
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
    graduated
  from {{ source('raw','raw_dataanalyst_intake') }}
),

cleaned as (
  select
    id_no,
    submitted_at,
    
    -- Standardize age_range
    case 
      when trim(lower(age_range)) like '%18-24%' then '18-24'
      when trim(lower(age_range)) like '%25-34%' then '25-34'
      when trim(lower(age_range)) like '%35-44%' then '35-44'
      when trim(lower(age_range)) like '%45-54%' then '45-54'
      when trim(lower(age_range)) like '%55%' then '55+'
      else trim(age_range)
    end as age_range,
    
    -- Standardize gender
    case 
      when trim(lower(gender)) in ('male', 'm') then 'Male'
      when trim(lower(gender)) in ('female', 'f') then 'Female'
      when trim(lower(gender)) in ('other', 'prefer not to say', 'non-binary') then 'Other'
      else nullif(trim(gender), '')
    end as gender,
    
    -- Clean country
    initcap(nullif(trim(country), '')) as country,
    
    -- Standardize referral_source to snake_case
    case 
      when trim(lower(referral_source)) like '%word of mouth%' then 'word_of_mouth'
      when trim(lower(referral_source)) like '%whatsapp%' then 'whatsapp'
      when trim(lower(referral_source)) like '%facebook%' then 'facebook'
      when trim(lower(referral_source)) like '%instagram%' then 'instagram'
      when trim(lower(referral_source)) like '%linkedin%' then 'linkedin'
      when trim(lower(referral_source)) like '%google%' then 'google_search'
      when trim(lower(referral_source)) like '%website%' then 'website'
      when trim(lower(referral_source)) like '%email%' then 'email'
      when trim(lower(referral_source)) like '%social media%' then 'social_media'
      else lower(replace(trim(referral_source), ' ', '_'))
    end as referral_source,
    
    -- Categorize years_experience into standardized snake_case values
    case 
      when trim(lower(years_experience)) like '%less than%six months%' or 
           trim(lower(years_experience)) like '%0-6 months%' or
           trim(lower(years_experience)) = 'none' then 'less_than_6_months'
      when trim(lower(years_experience)) like '%6 months%1 year%' or
           trim(lower(years_experience)) like '%6-12 months%' then '6_months_1_year'
      when trim(lower(years_experience)) like '%1-3 year%' or
           trim(lower(years_experience)) like '%1 to 3 year%' or
           trim(lower(years_experience)) like '%1-2 year%' then '1_3_years'
      when trim(lower(years_experience)) like '%4-6 year%' or
           trim(lower(years_experience)) like '%3-5 year%' or
           trim(lower(years_experience)) like '%4 to 6 year%' then '4_6_years'
      when trim(lower(years_experience)) like '%more than%' or
           trim(lower(years_experience)) like '%7+%' or
           trim(lower(years_experience)) like '%over%' then 'more_than_6_years'
      else lower(replace(trim(years_experience), ' ', '_'))
    end as years_experience,
    
    -- Standardize track to snake_case
    case 
      when trim(lower(track)) like '%data science%' then 'data_science'
      when trim(lower(track)) like '%data analytics%' then 'data_analytics'
      when trim(lower(track)) like '%data analyst%' then 'data_analytics'
      when trim(lower(track)) like '%machine learning%' then 'machine_learning'
      when trim(lower(track)) like '%ai%' or trim(lower(track)) like '%artificial intelligence%' then 'artificial_intelligence'
      when trim(lower(track)) like '%business intelligence%' then 'business_intelligence'
      when trim(lower(track)) like '%business analyst%' then 'business_analytics'
      else lower(replace(trim(track), ' ', '_'))
    end as track,
    
    -- Categorize weekly_commitment into snake_case values
    case 
      when trim(lower(weekly_commitment)) like '%less than%6%' or
           trim(lower(weekly_commitment)) like '%under 6%' or
           trim(lower(weekly_commitment)) like '%0-6%' then 'less_than_6_hours'
      when trim(lower(weekly_commitment)) like '%6-10%' or
           trim(lower(weekly_commitment)) like '%6 to 10%' then '6_10_hours'
      when trim(lower(weekly_commitment)) like '%10-14%' or
           trim(lower(weekly_commitment)) like '%10 to 14%' then '10_14_hours'
      when trim(lower(weekly_commitment)) like '%more than 14%' or
           trim(lower(weekly_commitment)) like '%over 14%' or
           trim(lower(weekly_commitment)) like '%14+%' then 'more_than_14_hours'
      else lower(replace(trim(weekly_commitment), ' ', '_'))
    end as weekly_commitment,
    
    -- Standardize main_aim to snake_case with your specific categories
    case 
      when trim(lower(main_aim)) like '%upskill%' then 'upskill'
      when trim(lower(main_aim)) like '%build%project%portfolio%' or
           trim(lower(main_aim)) like '%portfolio%' then 'build_project_portfolio'
      when trim(lower(main_aim)) like '%learn%data%afresh%' or
           trim(lower(main_aim)) like '%learn%from%scratch%' or
           trim(lower(main_aim)) like '%start%learning%data%' then 'learn_data_afresh'
      when trim(lower(main_aim)) like '%connect%fellow%data%professional%' or
           trim(lower(main_aim)) like '%network%' or
           trim(lower(main_aim)) like '%connect%professional%' then 'connect_with_professionals'
      when trim(lower(main_aim)) like '%career change%' then 'career_change'
      when trim(lower(main_aim)) like '%new career%' then 'career_change'
      else lower(replace(trim(main_aim), ' ', '_'))
    end as main_aim,
    
    -- Clean motivation (keep as is but trim)
    nullif(trim(motivation), '') as motivation,
    
    -- Standardize skill_level into snake_case categories
    case 
      when trim(lower(skill_level)) like '%beginner%' or
           trim(lower(skill_level)) like '%no%experience%' or
           trim(lower(skill_level)) like '%no learning%' then 'beginner'
      when trim(lower(skill_level)) like '%elementary%' or
           trim(lower(skill_level)) like '%basic%' or
           trim(lower(skill_level)) like '%theoretical%' then 'elementary'
      when trim(lower(skill_level)) like '%intermediate%' or
           trim(lower(skill_level)) like '%some experience%' then 'intermediate'
      when trim(lower(skill_level)) like '%advanced%' or
           trim(lower(skill_level)) like '%expert%' then 'advanced'
      else lower(replace(trim(skill_level), ' ', '_'))
    end as skill_level,
    
    -- aptitude_test_completed is already text, clean it properly
    case
      when trim(lower(aptitude_test_completed)) in ('yes', 'true', '1', 'completed', 'done') then true
      when trim(lower(aptitude_test_completed)) in ('no', 'false', '0', 'not completed', 'pending') then false
      else null
    end as aptitude_test_completed,
    
    -- total_score is already numeric, just ensure it's within reasonable bounds
    case 
      when total_score >= 0 and total_score <= 100 then round(total_score::numeric, 2)
      else null
    end as total_score,
    
    -- graduated is already boolean, keep as is
    coalesce(graduated, false) as graduated

  from raw
)

select * from cleaned