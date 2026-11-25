with source as (
    select * from {{ source('nsw_property', 'silver_property_sales') }}
),

renamed as (
    select
        -- keys
        property_id,
        dealing_number,
        district_code,

        -- address
        address,
        unit_number,
        house_number,
        street_name,
        initcap(suburb) as suburb,
        postcode,

        -- property
        area_sqm,
        nature_of_property,
        primary_purpose,
        case
            when nature_of_property = 'V' then 'vacant_land'
            when nature_of_property = 'R' and unit_number is not null then 'unit'
            when nature_of_property = 'R' then 'house'
            else 'other'
        end as property_type,

        -- sale
        contract_date,
        settlement_date,
        purchase_price,
        purchaser_count,
        vendor_count,
        legal_description,

        -- metadata
        download_ts,
        _source_file as source_file
    from source
)

select * from renamed