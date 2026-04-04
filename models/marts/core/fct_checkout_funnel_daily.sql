/*
    Daily checkout funnel metrics.
    One row per (date, merchant, country, device_type).
    Primary mart used for funnel dashboards and A/B test measurement.
*/

with funnel as (
    select * from {{ ref('int_checkout_funnel') }}
),

daily as (
    select
        date(session_started_at)            as session_date,
        merchant_id,
        country_code,
        device_type,
        platform,

        -- volume
        count(distinct session_id)          as total_sessions,

        -- funnel step counts
        sum(reached_cart_view)              as sessions_cart_view,
        sum(reached_payment_selection)      as sessions_payment_selection,
        sum(reached_personal_details)       as sessions_personal_details,
        sum(reached_credit_check)           as sessions_credit_check,
        sum(reached_confirmation)           as sessions_confirmation,

        -- conversion rates (relative to total_sessions)
        safe_divide(
            sum(reached_confirmation),
            count(distinct session_id)
        )                                   as checkout_conversion_rate,

        -- step-over-step drop ratios
        safe_divide(
            sum(reached_payment_selection),
            nullif(sum(reached_cart_view), 0)
        )                                   as cart_to_payment_rate,

        safe_divide(
            sum(reached_credit_check),
            nullif(sum(reached_personal_details), 0)
        )                                   as details_to_credit_check_rate,

        safe_divide(
            sum(reached_confirmation),
            nullif(sum(reached_credit_check), 0)
        )                                   as credit_check_to_confirmation_rate,

        -- cart value
        avg(cart_value_usd)                 as avg_cart_value_usd,
        sum(cart_value_usd)                 as total_cart_value_usd,

        -- utm attribution
        countif(utm_source = 'paid_social')  as sessions_paid_social,
        countif(utm_source = 'organic')      as sessions_organic,
        countif(utm_source = 'email')        as sessions_email

    from funnel
    group by 1, 2, 3, 4, 5
)

select * from daily
