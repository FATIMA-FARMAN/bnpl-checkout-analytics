with source as (
    select * from {{ source('bnpl_raw', 'orders') }}
),

renamed as (
    select
        order_id,
        session_id,
        customer_id,
        merchant_id,
        application_id,
        lower(trim(order_status))                       as order_status,
        cast(gross_amount_usd as numeric)               as gross_amount_usd,
        cast(refund_amount_usd as numeric)              as refund_amount_usd,
        cast(gross_amount_usd as numeric)
            - coalesce(cast(refund_amount_usd as numeric), 0)
                                                        as realized_amount_usd,
        lower(trim(installment_plan))                   as installment_plan,
        cast(is_new_customer as bool)                   as is_new_customer,
        cast(ordered_at as timestamp)                   as ordered_at,
        cast(created_at as timestamp)                   as created_at

    from source
    where order_id is not null
      and order_status != 'test'
)

select * from renamed
