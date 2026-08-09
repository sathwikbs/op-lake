-- Silver: cleaned & typed orders.
with src as (
    select * from {{ source('bronze', 'orders') }}
)

select
    cast(order_id    as bigint)         as order_id,
    cast(customer_id as bigint)         as customer_id,
    cast(order_ts    as timestamp)      as order_ts,
    upper(trim(status))                 as status,
    cast(amount      as decimal(12, 2)) as amount,
    upper(trim(currency))               as currency
from src
where order_id is not null
