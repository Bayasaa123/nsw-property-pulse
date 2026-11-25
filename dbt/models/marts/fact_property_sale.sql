with sales as (

    select * from {{ ref('stg_property_sales') }}
)

select 
    -- keys
    md5(concat_ws('|', property_id, dealing_number)) as sale_key,
    md5(concat_ws('|', suburb, postcode)) as suburb_key,
    property_id,
    dealing_number,

    -- dates
    contract_date,
    settlement_date,

    -- attributes
    property_type,
    address,

    -- measures
    purchase_price,
    area_sqm,
    case
        when property_type in ('house', 'vacant_land') and area_sqm > 0
            then round(purchase_price / area_sqm, 2)
    end as price_per_sqm,
    datediff(settlement_date, contract_date) as days_to_settle,
    purchaser_count,
    vendor_count
from sales