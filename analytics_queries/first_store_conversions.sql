select
  product_name,
  count(*) as num_first_conversions
from prod_gold.core.dim_device device
inner join prod_gold.core.fct_purchases purchases
on device.device_id = purchases.device_id and device.first_purchase_at_local = purchases.purchased_at_local
group by all
order by num_first_conversions desc
limit 10