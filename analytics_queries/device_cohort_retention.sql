/* in a prod environment, I'd probably make this a presentation layer table, but for now it's an ad hoc query.
Also the reason these have hard coded table refs is because they are the basis behind a databricks dashboard, not a dbt model.
*/

-- determines the cohorts by their first session, note that's not the same as first event
with first_sessions as (
  select
    device_id,
    cast(min(session_start_at_local) as date) as cohort_date
  from prod_gold.core.fct_sessions
  group by all
),
cohort_size as (
  select
    cohort_date,
    count(distinct device_id) as cohort_size
  from first_sessions
  group by all
),
-- calculates the days until they return
returns as (
  select
    fs.device_id,
    fs.cohort_date,
    datediff(cast(s.session_start_at_local as date), fs.cohort_date) as days_since
  from first_sessions fs
  inner join prod_gold.core.fct_sessions s
    on s.device_id = fs.device_id
),
retention as (
  select
    returns.cohort_date,
    returns.days_since,
    count(distinct returns.device_id) as retained_users
  from returns
  -- for simplification just look at first 30 days, this should be added as a parameter/filter
  where returns.days_since between 0 and 30
  group by all
)
select
  retention.cohort_date,
  retention.days_since,
  round((retention.retained_users / cs.cohort_size)*100, 2) as retention_pct,
  cs.cohort_size,
  retention.retained_users
from retention
inner join cohort_size cs using (cohort_date)
order by retention.cohort_date, retention.days_since