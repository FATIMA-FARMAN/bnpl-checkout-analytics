with source as (
    select * from {{ source('bnpl_raw', 'checkout_steps') }}
),

renamed as (
    select
        step_id,
        session_id,
        user_id,
        lower(trim(step_name))                  as step_name,
        step_sequence,
        cast(step_entered_at as timestamp)       as step_entered_at,
        cast(step_completed_at as timestamp)     as step_completed_at,
        lower(trim(step_status))                 as step_status,
        -- derive time spent on step in seconds
        timestamp_diff(
            cast(step_completed_at as timestamp),
            cast(step_entered_at as timestamp),
            second
        )                                        as time_on_step_seconds
    from source
)

select * from renamed
