/*
    Merchant dimension table enriched with trailing-30-day
    performance metrics (GMV, order count, approval rate).
*/

with merchants as (
    select * from {{ ref('stg_merchants') }}
),

gmv as (
    select
        merchant_id,
        sum(realized_gmv_usd)   as trailing_30d_gmv_usd,
        sum(total_orders)       as trailing_30d_orders
    from {{ ref('fct_gmv_daily') }}
    where order_date >= date_sub(current_date(), interval 30 day)
    group by 1
),

approval as (
    select
        merchant_id,
        avg(approval_rate)      as trailing_30d_approval_rate
    from {{ ref('fct_approval_rates_daily') }}
    where session_date >= date_sub(current_date(), interval 30 day)
    group by 1
),

final as (
    select
        m.merchant_id,
        m.merchant_name,
        m.merchant_category,
        m.country_code,
        m.onboarded_at,
        m.merchant_status,
        m.integration_type,
        coalesce(g.trailing_30d_gmv_usd, 0)          as trailing_30d_gmv_usd,
        coalesce(g.trailing_30d_orders, 0)            as trailing_30d_orders,
        coalesce(a.trailing_30d_approval_rate, 0)     as trailing_30d_approval_rate
    from merchants m
    left join gmv g using (merchant_id)
    left join approval a using (merchant_id)
)

select * from final
