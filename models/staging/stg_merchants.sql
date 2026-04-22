with source as (
    select * from {{ source('bnpl_raw', 'merchants') }}
),

renamed as (
    select
        merchant_id,
        lower(trim(merchant_name))                      as merchant_name,
        lower(trim(merchant_category))                  as merchant_category,
        lower(trim(country_code))                       as country_code,
        lower(trim(merchant_tier))                      as merchant_tier,
        cast(gmv_target_usd as numeric)                 as gmv_target_usd,
        cast(is_active as bool)                         as is_active,
        cast(onboarded_at as timestamp)                 as onboarded_at

    from source
    where merchant_id is not null
      and is_active = true
)

select * from renamed
