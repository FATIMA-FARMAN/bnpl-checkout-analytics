/*
    Builds a wide funnel table: one row per checkout session,
    with a flag for each step reached.
    Steps in order:
      1. cart_view
      2. payment_method_selection
      3. personal_details
      4. credit_check
      5. confirmation
*/

with sessions as (
    select * from {{ ref('stg_checkout_sessions') }}
),

steps as (
    select * from {{ ref('stg_checkout_steps') }}
),

step_flags as (
    select
        session_id,
        max(case when step_name = 'cart_view'                  then 1 else 0 end) as reached_cart_view,
        max(case when step_name = 'payment_method_selection'   then 1 else 0 end) as reached_payment_selection,
        max(case when step_name = 'personal_details'           then 1 else 0 end) as reached_personal_details,
        max(case when step_name = 'credit_check'               then 1 else 0 end) as reached_credit_check,
        max(case when step_name = 'confirmation'               then 1 else 0 end) as reached_confirmation,

        -- last completed step
        max(case when step_status = 'completed' then step_sequence else 0 end)    as last_completed_step_seq,

        -- drop-off step: first step entered but not completed
        min(case when step_status = 'abandoned' then step_name else null end)     as dropoff_step
    from steps
    group by 1
),

joined as (
    select
        s.session_id,
        s.user_id,
        s.merchant_id,
        s.session_started_at,
        s.cart_value_usd,
        s.device_type,
        s.platform,
        s.country_code,
        s.utm_source,
        s.utm_campaign,

        coalesce(f.reached_cart_view, 0)           as reached_cart_view,
        coalesce(f.reached_payment_selection, 0)   as reached_payment_selection,
        coalesce(f.reached_personal_details, 0)    as reached_personal_details,
        coalesce(f.reached_credit_check, 0)        as reached_credit_check,
        coalesce(f.reached_confirmation, 0)        as reached_confirmation,
        coalesce(f.last_completed_step_seq, 0)     as last_completed_step_seq,
        f.dropoff_step
    from sessions s
    left join step_flags f using (session_id)
)

select * from joined
