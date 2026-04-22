-- Daily credit approval / decline rates by merchant and installment plan.

with approvals as (
    select * from {{ ref('int_approval_rates') }}
),

daily as (
    select
        session_date,
        merchant_id,
        country_code,
        installment_plan,

        count(distinct application_id)                                          as total_applications,
        countif(credit_approved)                                                as approved_count,
        countif(credit_declined)                                                as declined_count,

        round(countif(credit_approved) * 100.0
              / nullif(count(distinct application_id), 0), 2)                  as approval_rate_pct,

        -- Decline reasons
        countif(decline_reason = 'insufficient_history')                        as declined_insufficient_history,
        countif(decline_reason = 'high_risk')                                   as declined_high_risk,
        countif(decline_reason = 'amount_exceeded')                             as declined_amount_exceeded,
        countif(decline_reason = 'fraud_flag')                                  as declined_fraud_flag,

        -- Latency
        round(avg(decision_latency_seconds), 0)                                 as avg_decision_latency_seconds,
        round(avg(case when credit_approved then approved_amount_usd end), 2)   as avg_approved_amount_usd,
        round(avg(requested_amount_usd), 2)                                     as avg_requested_amount_usd

    from approvals
    group by 1, 2, 3, 4
)

select * from daily
