/*
    Joins sessions to loan applications to compute per-session
    credit decision outcomes. Used downstream in the approval
    rate mart.
*/

with sessions as (
    select * from {{ ref('stg_checkout_sessions') }}
),

applications as (
    select * from {{ ref('stg_loan_applications') }}
),

joined as (
    select
        s.session_id,
        s.user_id,
        s.merchant_id,
        s.cart_value_usd,
        s.country_code,
        s.device_type,
        s.session_started_at,
        date(s.session_started_at)              as session_date,

        a.application_id,
        a.decision_status,
        a.decline_reason,
        a.risk_score,
        a.requested_amount_usd,
        a.approved_amount_usd,
        a.decision_latency_seconds,
        a.installment_plan,

        case when a.decision_status = 'approved' then 1 else 0 end   as is_approved,
        case when a.decision_status = 'declined' then 1 else 0 end   as is_declined,
        case when a.application_id is null       then 1 else 0 end   as no_application
    from sessions s
    left join applications a using (session_id)
)

select * from joined
