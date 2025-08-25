with exchange_rates as (select * from {{source("tocaboca", "exchange_rates")}}),

-- fetch relevant currency codes to keep table more usable
events as (select currency_code from {{ ref("stg_events")}} group by all),

--  I have ignored "is_extrapolated" here, but if doing actual financial analysis, it should be kept in
final as (
    select
        dt as date,
        currency_code,
        cast(usd_per_currency as float) as usd_per_currency
    from exchange_rates
    inner join events using (currency_code)
)

select * from final