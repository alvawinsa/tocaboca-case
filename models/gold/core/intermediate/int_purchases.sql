{{config(
    materialized='ephemeral'
)}}

with events as (select * from {{ ref("stg_events") }}where event_name = 'in_app_purchase'),

exchange_rates as (
    select * from {{ ref("stg_exchange_rates") }}
),

final as (
select
    from_utc_timestamp(event_timestamp, 'Europe/Stockholm') as purchased_at_local,
    coalesce(device_id, install_id) as device_id,
    price * usd_per_currency as price_usd,
    lower(product_name) as product_name,
    --quantity seems to always be 1, so assuming it may never bundle items even of the same kind, I would fix this in a prod model
    quantity
from events
left join exchange_rates on events.currency_code = exchange_rates.currency_code
    and events.event_date = exchange_rates.date
)

select * from final
