{{ config(materialized='view') }}

with base as (
    select
        product_id,
        event_type,
        count(*) as cnt
    from {{ ref('slv_clickstream') }}
    group by 1,2
),
pivoted as (
    select
        product_id,
        sum(case when event_type = 'product_view' then cnt end) as product_views,
        sum(case when event_type = 'add_to_cart' then cnt end) as cart_adds,
        sum(case when event_type = 'purchase' then cnt end) as purchases
    from base
    group by product_id
)
select 
    product_id,
    product_views,
    cart_adds,
    purchases,
    safe_divide(cart_adds, product_views) as view_to_cart_rate,
    safe_divide(purchases, cart_adds)     as cart_to_purchase_rate,
    safe_divide(purchases, product_views) as view_to_purchase_rate
from pivoted