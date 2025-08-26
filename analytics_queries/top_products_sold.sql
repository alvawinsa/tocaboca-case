select
    product_name,
    sum(quantity) as num_products
from prod_gold.core.fct_purchases
group by all
order by num_products desc
limit 10