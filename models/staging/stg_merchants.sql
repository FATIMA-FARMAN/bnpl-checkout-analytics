with source as (
    select * from {{ source('bnpl_raw', 'merchants') }}
),

renamed as (
    select
        merchant_id,
        merchant_name,
        merchant_category,
        country_code,
        cast(onboarded_at as timestamp)          as onboarded_at,
        lower(trim(merchant_status))             as merchant_status,
        integration_type   -- api / plugin / hosted
    from source
)

select * from renamed
