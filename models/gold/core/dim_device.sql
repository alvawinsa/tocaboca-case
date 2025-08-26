with events as (select * from {{ ref("stg_events") }}),

    int_purchases as (
        select
            device_id,
            sum(price_usd) as total_usd_spent,
            sum(quantity) as total_products,
            true as has_purchased
        from {{ref("int_purchases")}}
        group by all
    ),

    final as (
        select
            device_id,
            max_by(device_category, event_timestamp) as device_category,
            max_by(install_source, event_timestamp) as install_source,
            /* note this is not the same as their first session, I'd probably add that too */
            from_utc_timestamp(min(event_timestamp), 'Europe/Stockholm') as first_event_at_local,
            from_utc_timestamp(max(event_timestamp), 'Europe/Stockholm') as last_event_at_local,
            max(ga_session_number) as number_of_sessions,
            coalesce(max(subscription), false) as has_subscription,
            coalesce(sum(total_usd_spent), 0) as total_usd_spent,
            coalesce(sum(total_products), 0) as total_products,
            coalesce(has_purchased, false) as has_purchased
        from events
        left join int_purchases using (device_id)
        group by all
    )

select * from final
