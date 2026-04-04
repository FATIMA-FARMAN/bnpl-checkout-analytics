/*
    Daily credit approval metrics.
    One row per (date, merchant, country, installment_plan).
    Tracks approval rates, decline reasons, and risk score distributions.
*/

with approvals as (
    select * from {{ ref('int_approval_rates') }}
),

daily as (
    select
        session_date,
        merchant_id,
        country_code,
        device_type,
        installment_plan,

        count(distinct session_id)           as total_sessions_with_application,
        count(distinct application_id)       as total_applications,

        -- approval/decline counts
        sum(is_approved)                     as approved_count,
        sum(is_declined)                     as declined_count,

        -- rates
        safe_divide(
            sum(is_approved),
            count(distinct application_id)
        )                                    as approval_rate,

        safe_divide(
            sum(is_declined),
            count(distinct application_id)
        )                                    as decline_rate,

        -- risk score stats
        avg(risk_score)                      as avg_risk_score,
        min(risk_score)                      as min_risk_score,
        max(risk_score)                      as max_risk_score,

        -- decision speed
        avg(decision_latency_seconds)        as avg_decision_latency_seconds,

        -- amount stats
        avg(requested_amount_usd)            as avg_requested_amount_usd,
        avg(approved_amount_usd)             as avg_approved_amount_usd,

        -- top decline reasons
        countif(decline_reason = 'insufficient_credit_history')  as declined_insufficient_history,
        countif(decline_reason = 'high_risk_score')              as declined_high_risk,
        countif(decline_reason = 'amount_exceeded')              as declined_amount_exceeded,
        countif(decline_reason = 'fraud_flag')                   as declined_fraud_flag

    from approvals
    where application_id is not null
    group by 1, 2, 3, 4, 5
)

select * from daily
