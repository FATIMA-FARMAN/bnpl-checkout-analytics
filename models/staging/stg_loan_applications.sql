with source as (
    select * from {{ source('bnpl_raw', 'loan_applications') }}
),

renamed as (
    select
        application_id,
        session_id,
        customer_id,
        merchant_id,
        cast(requested_amount_usd as numeric)               as requested_amount_usd,
        cast(approved_amount_usd as numeric)                as approved_amount_usd,
        lower(trim(decision))                               as decision,
        lower(trim(decline_reason))                         as decline_reason,
        lower(trim(installment_plan))                       as installment_plan,
        cast(submitted_at as timestamp)                     as submitted_at,
        cast(decided_at as timestamp)                       as decided_at,
        timestamp_diff(
            cast(decided_at as timestamp),
            cast(submitted_at as timestamp),
            second
        )                                                   as decision_latency_seconds,
        cast(created_at as timestamp)                       as created_at

    from source
    where application_id is not null
)

select * from renamed
