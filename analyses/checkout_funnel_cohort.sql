/*
    Ad-hoc analysis: weekly cohort funnel
    Shows how checkout conversion has trended week-over-week.
    Use this in BigQuery or export to Looker for dashboards.
*/

select
    date_trunc(session_date, week)              as week_start,
    country_code,
    device_type,

    sum(total_sessions)                         as total_sessions,
    sum(sessions_cart_view)                     as cart_views,
    sum(sessions_payment_selection)             as payment_selections,
    sum(sessions_personal_details)              as personal_details,
    sum(sessions_credit_check)                  as credit_checks,
    sum(sessions_confirmation)                  as confirmations,

    -- week-level conversion
    safe_divide(
        sum(sessions_confirmation),
        sum(total_sessions)
    )                                           as weekly_conversion_rate,

    -- biggest drop-off ratio (cart → payment)
    safe_divide(
        sum(sessions_cart_view) - sum(sessions_payment_selection),
        nullif(sum(sessions_cart_view), 0)
    )                                           as cart_to_payment_dropoff_rate

from {{ ref('fct_checkout_funnel_daily') }}
where session_date >= date_sub(current_date(), interval 90 day)
group by 1, 2, 3
order by 1 desc, weekly_conversion_rate desc
