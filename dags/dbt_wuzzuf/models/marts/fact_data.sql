select 
    company,
    count(*) as total_jobs,
    listagg(job,'----')within group(order by job) as all_jobs,
    listagg(salary,'----')within group(order by salary)as all_salary,
    city
from {{ ref('jobs_stg') }}
where company is not null
group by company,city