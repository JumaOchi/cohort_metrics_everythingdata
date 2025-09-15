

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
  from "postgres"."raw"."raw_datascience_intake"
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
    
    -- Standardize referral_source
    case 
      when trim(lower(referral_source)) like '%word of mouth%' then 'Word of Mouth'
      when trim(lower(referral_source)) like '%whatsapp%' then 'WhatsApp'
      when trim(lower(referral_source)) like '%facebook%' then 'Facebook'
      when trim(lower(referral_source)) like '%instagram%' then 'Instagram'
      when trim(lower(referral_source)) like '%linkedin%' then 'LinkedIn'
      when trim(lower(referral_source)) like '%google%' then 'Google Search'
      when trim(lower(referral_source)) like '%website%' then 'Website'
      when trim(lower(referral_source)) like '%email%' then 'Email'
      else initcap(nullif(trim(referral_source), ''))
    end as referral_source,
    
    -- Categorize years_experience into ordinal values
    case 
      when trim(lower(years_experience)) like '%less than%six months%' or 
           trim(lower(years_experience)) like '%0-6 months%' or
           trim(lower(years_experience)) = 'none' then 'Less than 6 months'
      when trim(lower(years_experience)) like '%6 months%1 year%' or
           trim(lower(years_experience)) like '%6-12 months%' then '6 months - 1 year'
      when trim(lower(years_experience)) like '%1-2 year%' or
           trim(lower(years_experience)) like '%1 to 2 year%' then '1-2 years'
      when trim(lower(years_experience)) like '%2-3 year%' or
           trim(lower(years_experience)) like '%2 to 3 year%' then '2-3 years'
      when trim(lower(years_experience)) like '%3-5 year%' or
           trim(lower(years_experience)) like '%3 to 5 year%' then '3-5 years'
      when trim(lower(years_experience)) like '%more than 5%' or
           trim(lower(years_experience)) like '%5+ year%' or
           trim(lower(years_experience)) like '%over 5%' then '5+ years'
      else trim(years_experience)
    end as years_experience,
    
    -- Standardize track
    case 
      when trim(lower(track)) like '%data science%' then 'Data Science'
      when trim(lower(track)) like '%data analytics%' then 'Data Analytics'
      when trim(lower(track)) like '%machine learning%' then 'Machine Learning'
      when trim(lower(track)) like '%ai%' or trim(lower(track)) like '%artificial intelligence%' then 'AI'
      else initcap(nullif(trim(track), ''))
    end as track,
    
    -- Categorize weekly_commitment into ordinal values
    case 
      when trim(lower(weekly_commitment)) like '%less than%6%' or
           trim(lower(weekly_commitment)) like '%under 6%' or
           trim(lower(weekly_commitment)) like '%0-6%' then 'Less than 6 hours'
      when trim(lower(weekly_commitment)) like '%6-10%' or
           trim(lower(weekly_commitment)) like '%6 to 10%' then '6-10 hours'
      when trim(lower(weekly_commitment)) like '%10-14%' or
           trim(lower(weekly_commitment)) like '%10 to 14%' then '10-14 hours'
      when trim(lower(weekly_commitment)) like '%more than 14%' or
           trim(lower(weekly_commitment)) like '%over 14%' or
           trim(lower(weekly_commitment)) like '%14+%' then 'More than 14 hours'
      else trim(weekly_commitment)
    end as weekly_commitment,
    
    -- Standardize main_aim
    case 
      when trim(lower(main_aim)) like '%upskill%' then 'Upskill'
      when trim(lower(main_aim)) like '%career change%' then 'Career Change'
      when trim(lower(main_aim)) like '%new career%' then 'Career Change'
      when trim(lower(main_aim)) like '%promotion%' then 'Career Advancement'
      when trim(lower(main_aim)) like '%advance%' then 'Career Advancement'
      when trim(lower(main_aim)) like '%start%business%' then 'Start Business'
      when trim(lower(main_aim)) like '%freelanc%' then 'Freelancing'
      else initcap(nullif(trim(main_aim), ''))
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