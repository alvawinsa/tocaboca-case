/* Depending on how we ingest this data, and how it's sent, it may make sense to
build a snapshot on this table, in order not to lose historical info on what products
we had, if rows get overwritten */

with products as (select * from {{source("tocaboca", "products")}}),

final as (
    select
        lower(product_name) as product_name,
        lower(type) as product_type,
        lower(subtype) as product_subtype
    from products
)

select * from final