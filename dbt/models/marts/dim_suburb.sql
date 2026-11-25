with sales as (
    select * from {{ ref('stg_property_sales') }}
),

suburbs as (

    select distinct 
        suburb,
        postcode
    from sales
    where suburb is not null
)

select
    md5(concat_ws('|', suburb, postcode)) as suburb_key,
    suburb,
    postcode
from suburbs