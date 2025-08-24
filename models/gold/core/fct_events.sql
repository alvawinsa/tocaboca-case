{{
    config(
        materialized='incremental'
    )
}}
with events as (select * from {{ ref("stg_events") }}
-- only scan latest two days for efficiency
{% if is_incremental() %}
        where event_timestamp >= dateadd(day, -2, current_date)
{% endif %}
),



final as (
    select
        md5(concat_ws(ga_session_id, device_id, install_id)) as session_id,
        from_utc_timestamp(event_timestamp, 'Europe/Stockholm') as event_timestamp_local,
        md5(concat_ws(device_id, install_id)) as device_key,
        event_name,
        engaged_session_event,
        is_conversion_event,
        event_origin,
        screen_class,
        ga_session_number as session_number,
        has_subscription
    from events
)

select * from final
-- check latest timestamp to only add new records since then
{% if is_incremental() %}
    where event_timestamp_local >= (
        select coalesce(max(event_timestamp_local), '2000-01-01')
        from {{ this }}
    )
{% endif %}