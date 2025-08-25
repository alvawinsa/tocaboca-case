with events as (select * from {{ ref("stg_events") }}),

final as (
    select
        /* there are probably cleaner ways of doing this, e.g. mapping early events install_id to later
        events install_id where device_id is populated */
        coalesce(device_id, install_id) as device_id,
        max_by(device_category, event_timestamp) as device_category,
        max_by(install_source, event_timestamp) as install_source,
        from_utc_timestamp(min(event_timestamp), 'Europe/Stockholm') as first_event_at_local,
        from_utc_timestamp(max(event_timestamp), 'Europe/Stockholm') as last_event_at_local,
        max(ga_session_number) as number_of_sessions,
        coalesce(max(subscription), false) as has_subscription
    from events
    group by all
)

select * from final
