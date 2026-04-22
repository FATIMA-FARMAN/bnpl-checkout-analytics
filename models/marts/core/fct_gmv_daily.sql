-- Daily GMV: realized vs gross, AOV, new vs returning customer split.

with orders as (
    select * from {{ ref('stg_orders') }}
),

daily as (
    select
        date(ordered_at)                                                        as order_date,
        merchant_id,
        installment_plan,

        count(distinct order_id)                                                as total_orders,

        -- GMV
        round(sum(gross_amount_usd), 2)                                         as gross_gmv_usd,
        round(sum(realized_amount_usd), 2)                                      as realized_gmv_usd,
        round(sum(refund_amount_usd), 2)                                        as refunded_amount_usd,
        round(
            sum(refund_amount_usd) * 100.0
            / nullif(sum(gross_amount_usd), 0), 2)                             as refund_rate_pct,

        -- AOV
        round(avg(gross_amount_usd), 2)                                         as gross_aov_usd,
        round(avg(realized_amount_usd), 2)                                      as realized_aov_usd,

        -- New vs returning
        countif(is_new_customer = true)                                         as new_customer_orders,
        countif(is_new_customer = false)                                        as returning_customer_orders,
        round(
            countif(is_new_customer = true) * 100.0
            / nullif(count(distinct order_id), 0), 2)                          as new_customer_share_pct,

        -- Cancellations
        countif(order_status = 'cancelled')                                     as cancelled_orders,
        countif(order_status in ('refunded', 'partially_refunded'))             as refunded_orders

    from orders
    where order_status != 'cancelled'
    group by 1, 2, 3
)

select * from daily
