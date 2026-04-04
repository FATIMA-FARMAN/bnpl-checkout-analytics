with source as (
    select * from {{ source('bnpl_raw', 'loan_applications') }}
),

renamed as (
    select
        application_id,
        session_id,
        user_id,
        requested_amount_usd,
        approved_amount_usd,
        cast(applied_at as timestamp)            as applied_at,
        cast(decision_at as timestamp)           as decision_at,
        lower(trim(decision_status))             as decision_status,   -- approved / declined / pending
        lower(trim(decline_reason))              as decline_reason,
        risk_score,
        installment_plan,   -- e.g. 3x, 4x, 6x
        interest_rate_pct,
        -- time from application to decision (seconds)
        timestamp_diff(
            cast(decision_at as timestamp),
            cast(applied_at as timestamp),
            second
        )                                        as decision_latency_seconds
    from source
)

select * from renamed
