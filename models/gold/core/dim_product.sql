with products as (select * from {{ ref("stg_products") }}),

final as (
    select
        product_name,
        product_type,
        product_subtype
    from products
)

select * from final