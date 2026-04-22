-- Merchant dimension with trailing 30-day performance metrics.

with merchants as (
    select * from {{ ref('stg_merchants') }}
),

gmv as (
    select
        merchant_id,
        sum(realized_gmv_usd)                                                   as trailing_30d_gmv_usd,
        sum(total_orders)                                                       as trailing_30d_orders,
        round(avg(gross_aov_usd), 2)                                            as trailing_30d_aov_usd,
        round(avg(refund_rate_pct), 2)                                          as trailing_30d_refund_rate_pct
    from {{ ref('fct_gmv_daily') }}
    where order_date >= date_sub(current_date(), interval 30 day)
    group by merchant_id
),

funnel as (
    select
        merchant_id,
        round(avg(conversion_rate_pct), 2)                                      as trailing_30d_conversion_rate_pct
    from {{ ref('fct_checkout_funnel_daily') }}
    where session_date >= date_sub(current_date(), interval 30 day)
    group by merchant_id
),

approvals as (
    select
        merchant_id,
        round(avg(approval_rate_pct), 2)                                        as trailing_30d_approval_rate_pct,
        round(avg(avg_decision_latency_seconds), 0)                             as trailing_30d_avg_decision_latency_seconds
    from {{ ref('fct_approval_rates_daily') }}
    where session_date >= date_sub(current_date(), interval 30 day)
    group by merchant_id
),

final as (
    select
        m.merchant_id,
        m.merchant_name,
        m.merchant_category,
        m.country_code,
        m.merchant_tier,
        m.gmv_target_usd,
        m.onboarded_at,

        coalesce(g.trailing_30d_gmv_usd, 0)                                     as trailing_30d_gmv_usd,
        coalesce(g.trailing_30d_orders, 0)                                      as trailing_30d_orders,
        g.trailing_30d_aov_usd,
        g.trailing_30d_refund_rate_pct,
        f.trailing_30d_conversion_rate_pct,
        a.trailing_30d_approval_rate_pct,
        a.trailing_30d_avg_decision_latency_seconds,

        -- Performance tier vs GMV target
        case
            when m.gmv_target_usd is null then 'no_target'
            when coalesce(g.trailing_30d_gmv_usd, 0) >= m.gmv_target_usd * 0.9 then 'on_track'
            when coalesce(g.trailing_30d_gmv_usd, 0) >= m.gmv_target_usd * 0.7 then 'at_risk'
            else 'off_track'
        end                                                                     as gmv_performance_status

    from merchants m
    left join gmv g using (merchant_id)
    left join funnel f using (merchant_id)
    left join approvals a using (merchant_id)
)

select * from final
