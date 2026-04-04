with source as (
    select * from {{ source('bnpl_raw', 'checkout_sessions') }}
),

renamed as (
    select
        session_id,
        user_id,
        merchant_id,
        cast(session_started_at as timestamp)  as session_started_at,
        cast(session_ended_at as timestamp)     as session_ended_at,
        cart_value_usd,
        currency,
        device_type,
        platform,
        country_code,
        utm_source,
        utm_medium,
        utm_campaign,
        lower(trim(session_status))             as session_status
    from source
)

select * from renamed
