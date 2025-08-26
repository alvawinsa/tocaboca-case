select
  date_trunc('week', session_start_at_local)::date as week,
  count(distinct device_id) as num_visitors
from prod_gold.core.fct_sessions
where has_entered_store is true
group by all
order by week desc