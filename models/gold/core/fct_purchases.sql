with int_purchases as (select * from {{ ref("int_purchases") }}),

    final as (
        select
            purchased_at_local,
            device_id,
            price_usd,
            product_name,
            quantity
        from int_purchases
    )

select * from final
