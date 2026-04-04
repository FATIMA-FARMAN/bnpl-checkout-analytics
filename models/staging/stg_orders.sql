with source as (
    select * from {{ source('bnpl_raw', 'orders') }}
),

renamed as (
    select
        order_id,
        session_id,
        user_id,
        merchant_id,
        application_id,
        gmv_usd,
        cast(order_placed_at as timestamp)       as order_placed_at,
        lower(trim(order_status))                as order_status,   -- completed / cancelled / refunded
        payment_method,
        installment_plan,
        is_first_order,
        cast(is_first_order as bool)             as is_new_customer
    from source
)

select * from renamed
