-- Wide funnel table: one row per session with step-reached flags
-- and drop-off identification. Consumed by fct_checkout_funnel_daily.

with sessions as (
    select * from {{ ref('stg_checkout_sessions') }}
),

steps as (
    select * from {{ ref('stg_checkout_steps') }}
),

step_flags as (
    select
        session_id,
        max(case when step_name = 'cart'            then 1 else 0 end) as reached_cart,
        max(case when step_name = 'customer_details' then 1 else 0 end) as reached_details,
        max(case when step_name = 'credit_check'    then 1 else 0 end) as reached_credit_check,
        max(case when step_name = 'payment_plan'    then 1 else 0 end) as reached_payment_plan,
        max(case when step_name = 'confirmation'    then 1 else 0 end) as reached_confirmation,
        -- Drop-off step = highest step reached where session did not complete
        max(case when step_completed = false then step_sequence else 0 end) as dropoff_sequence,
        max(case when step_completed = false then step_name else null end) as dropoff_step_name,
        sum(time_on_step_seconds)                                           as total_time_seconds

    from steps
    group by session_id
),

final as (
    select
        s.session_id,
        s.customer_id,
        s.merchant_id,
        s.country_code,
        s.device_type,
        s.session_status,
        s.cart_value_usd,
        s.session_started_at,
        date(s.session_started_at)                          as session_date,

        coalesce(f.reached_cart, 0)                         as reached_cart,
        coalesce(f.reached_details, 0)                      as reached_details,
        coalesce(f.reached_credit_check, 0)                 as reached_credit_check,
        coalesce(f.reached_payment_plan, 0)                 as reached_payment_plan,
        coalesce(f.reached_confirmation, 0)                 as reached_confirmation,

        case when s.session_status = 'completed' then true else false end as converted,
        f.dropoff_step_name,
        coalesce(f.total_time_seconds, 0)                   as total_time_seconds

    from sessions s
    left join step_flags f using (session_id)
)

select * from final
