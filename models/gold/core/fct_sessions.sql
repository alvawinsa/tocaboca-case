with events as (select * from {{ ref("stg_events") }}),

/* I would rename these to more sensible names if I had more context to what exactly they corresponded to */
with final as (
    select
        md5(concat_ws(ga_session_id, device_id, install_id)) as session_id,
        from_utc_timestamp(
            min(case when event_name = 'session_start' then event_timestamp end),
            'Europe/Stockholm'
        ) as session_start_at_local,
        from_utc_timestamp(max(event_timestamp), 'Europe/Stockholm') as session_end_at_local,
        unix_timestamp(max(event_timestamp)) - unix_timestamp(min(case when event_name = 'session_start' then event_timestamp end)) as session_duration_seconds,
        md5(concat_ws(device_id, install_id)) as device_key,
        install_source,
        -- these booleans have true/null in silver model, I would've cleaned that up to be true/false, but lack of time...
        max(case when event_name = 'store_impression' then true else false end) as has_store_impression,
        max(case when event_name = 'app_crashed' then true else false end) as has_app_crashed,
        max(case when event_name = 'store_entry' then true else false end) as has_entered_store,
        max(case when event_name = 'in_app_purchased' then true else false end) as has_in_app_purchased,
        max(case when is_session_engaged = true then true else false end) as is_session_engaged,
        max(case when has_subscription = true then true else false end) as has_subscription
    from events
    group by all
    )

select *
from final
where session_start_at is not null -- filter out all non-sessions
    and session_id is not null -- if we can't identify them it's not useful
