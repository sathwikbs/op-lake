-- Silver: cleaned & typed customers.
with src as (
    select * from {{ source('bronze', 'customers') }}
)

select
    cast(customer_id as bigint) as customer_id,
    trim(name)                  as name,
    lower(trim(email))          as email,
    upper(trim(country))        as country,
    cast(signup_date as date)   as signup_date
from src
where customer_id is not null
