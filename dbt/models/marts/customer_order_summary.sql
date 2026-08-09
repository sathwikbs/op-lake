-- Gold: per-customer order summary (business-ready aggregate).
with orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'COMPLETED'
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    c.customer_id,
    c.name,
    c.country,
    count(o.order_id)                    as completed_orders,
    coalesce(sum(o.amount), 0)           as total_amount,
    max(o.order_ts)                      as last_order_ts
from customers c
left join orders o
    on c.customer_id = o.customer_id
group by c.customer_id, c.name, c.country
