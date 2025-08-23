select
nameofjob as job,
REPLACE(nameofcompany,'-','') as company,
CASE
WHEN ARRAY_SIZE(SPLIT(location,','))=3 THEN TRIM(SPLIT_PART(location,',',1))
ELSE NULL
END AS district,
CASE 
  WHEN ARRAY_SIZE(SPLIT(location,',')) >=2 THEN TRIM(SPLIT_PART(location,',',2))
  ELSE TRIM(SPLIT_PART(location,',',1))
  END AS city,
TRIM(SPLIT_PART(location,',',-1)) as country,
CASE
 WHEN JOBANNONCMENTTIME IS NULL THEN 'more than 8 days'
 ELSE JOBANNONCMENTTIME
 END AS publish_date,
jobtype as job_type,
workmode as work_mode,
skills,
joblink as job_link,
salary,
job_skills as info_of_skills,
description



FROM {{ source('raw_data','JOBS') }}