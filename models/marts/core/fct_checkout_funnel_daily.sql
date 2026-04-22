-- Daily checkout funnel KPIs.
-- Primary mart for conversion rate dashboards.

with funnel as (
    select * from {{ ref('int_checkout_funnel') }}
),

daily as (
    select
        session_date,
        merchant_id,
        country_code,
        device_type,

        count(distinct session_id)                                              as total_sessions,

        -- Step reach counts
        sum(reached_cart)                                                       as sessions_reached_cart,
        sum(reached_details)                                                    as sessions_reached_details,
        sum(reached_credit_check)                                               as sessions_reached_credit_check,
        sum(reached_payment_plan)                                               as sessions_reached_payment_plan,
        sum(reached_confirmation)                                               as sessions_reached_confirmation,

        -- Conversions
        countif(converted = true)                                               as converted_sessions,
        round(countif(converted = true) * 100.0 / nullif(count(*), 0), 2)      as conversion_rate_pct,

        -- Step-over-step drop rates
        round(
            (sum(reached_cart) - sum(reached_details)) * 100.0
            / nullif(sum(reached_cart), 0), 2)                                  as cart_to_details_dropoff_pct,
        round(
            (sum(reached_details) - sum(reached_credit_check)) * 100.0
            / nullif(sum(reached_details), 0), 2)                               as details_to_credit_dropoff_pct,
        round(
            (sum(reached_credit_check) - sum(reached_payment_plan)) * 100.0
            / nullif(sum(reached_credit_check), 0), 2)                          as credit_to_payment_dropoff_pct,
        round(
            (sum(reached_payment_plan) - sum(reached_confirmation)) * 100.0
            / nullif(sum(reached_payment_plan), 0), 2)                          as payment_to_confirm_dropoff_pct,

        -- Engagement
        round(avg(total_time_seconds), 0)                                       as avg_session_duration_seconds,
        round(avg(cart_value_usd), 2)                                           as avg_cart_value_usd

    from funnel
    group by 1, 2, 3, 4
)

select * from daily
