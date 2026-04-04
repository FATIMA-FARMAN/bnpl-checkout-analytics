/*
    Daily GMV and order metrics.
    One row per (date, merchant, country, installment_plan).
    Core revenue tracking mart.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

merchants as (
    select * from {{ ref('stg_merchants') }}
),

joined as (
    select
        o.*,
        m.merchant_name,
        m.merchant_category,
        m.integration_type
    from orders o
    left join merchants m using (merchant_id)
),

daily as (
    select
        date(order_placed_at)               as order_date,
        merchant_id,
        merchant_name,
        merchant_category,
        country_code,
        installment_plan,
        integration_type,

        -- order counts
        count(distinct order_id)            as total_orders,
        countif(order_status = 'completed') as completed_orders,
        countif(order_status = 'cancelled') as cancelled_orders,
        countif(order_status = 'refunded')  as refunded_orders,
        countif(is_new_customer = true)     as new_customer_orders,

        -- GMV (only completed orders count toward realized GMV)
        sum(case when order_status = 'completed' then gmv_usd else 0 end)    as realized_gmv_usd,
        sum(gmv_usd)                        as gross_gmv_usd,

        -- AOV
        safe_divide(
            sum(case when order_status = 'completed' then gmv_usd else 0 end),
            nullif(countif(order_status = 'completed'), 0)
        )                                   as avg_order_value_usd,

        -- new vs returning split
        safe_divide(
            countif(is_new_customer = true),
            count(distinct order_id)
        )                                   as new_customer_share

    from joined
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from daily
