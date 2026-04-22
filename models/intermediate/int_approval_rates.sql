-- Sessions joined to credit decisions.
-- Enriches checkout funnel with approval outcome and decline reason.

with funnel as (
    select * from {{ ref('int_checkout_funnel') }}
),

applications as (
    select * from {{ ref('stg_loan_applications') }}
),

final as (
    select
        f.session_id,
        f.customer_id,
        f.merchant_id,
        f.country_code,
        f.session_date,
        f.cart_value_usd,
        f.converted,

        a.application_id,
        a.decision,
        a.decline_reason,
        a.installment_plan,
        a.requested_amount_usd,
        a.approved_amount_usd,
        a.decision_latency_seconds,

        case when a.decision = 'approved' then true else false end   as credit_approved,
        case when a.decision = 'declined' then true else false end   as credit_declined

    from funnel f
    left join applications a using (session_id)
    where f.reached_credit_check = 1
)

select * from final
