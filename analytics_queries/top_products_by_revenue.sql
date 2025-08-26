select
    product_name,
    sum(price_usd) as revenue
from prod_gold.core.fct_purchases
group by all
order by revenue desc
limit 10