with source as (
    select * from {{ source('bnpl_raw', 'checkout_sessions') }}
),

renamed as (
    select
        session_id,
        customer_id,
        merchant_id,
        lower(trim(country_code))                       as country_code,
        lower(trim(device_type))                        as device_type,
        lower(trim(session_status))                     as session_status,
        cast(session_started_at as timestamp)           as session_started_at,
        cast(session_ended_at as timestamp)             as session_ended_at,
        timestamp_diff(
            cast(session_ended_at as timestamp),
            cast(session_started_at as timestamp),
            second
        )                                               as session_duration_seconds,
        cast(cart_value_usd as numeric)                 as cart_value_usd,
        cast(created_at as timestamp)                   as created_at

    from source
    where session_id is not null
      and customer_id is not null
)

select * from renamed
