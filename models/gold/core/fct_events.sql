with events as (select * from {{ ref("stg_events") }}),

final as (
    select
        md5(concat_ws(ga_session_id, device_id, install_id)) as session_id,
        from_utc_timestamp(event_timestamp, 'Europe/Stockholm') as event_timestamp_local,
        device_id,
        install_id,
        event_name,
        engaged_session_event,
        is_conversion_event,
        event_origin,
        screen_class,
        ga_session_number as session_number,
        has_subscription
    from stg_events
)

select * from final