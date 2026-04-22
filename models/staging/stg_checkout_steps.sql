with source as (
    select * from {{ source('bnpl_raw', 'checkout_steps') }}
),

renamed as (
    select
        step_id,
        session_id,
        lower(trim(step_name))                          as step_name,
        step_sequence,
        cast(step_entered_at as timestamp)              as step_entered_at,
        cast(step_exited_at as timestamp)               as step_exited_at,
        timestamp_diff(
            cast(step_exited_at as timestamp),
            cast(step_entered_at as timestamp),
            second
        )                                               as time_on_step_seconds,
        lower(trim(exit_reason))                        as exit_reason,
        case when lower(trim(exit_reason)) = 'completed'
             then true else false end                   as step_completed

    from source
    where step_id is not null
      and session_id is not null
)

select * from renamed
