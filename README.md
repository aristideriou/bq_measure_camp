## 1 - Metric designer

## Basic count
```sql
SELECT
  count(*) as total_hits
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
```

## Basic count on filtered event name
```sql

SELECT
  count(*) as total_purchase
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'
```

## Basic count on filtered event name + filtered event_params
```sql
SELECT
  COUNT(*) as hits
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'add_to_cart'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') = 'https://shop.googlemerchandisestore.com/Google+Redesign/Apparel'
```

## Count on user_pseudo_id on filtered event name + filtered event_params
```sql
SELECT
  COUNT(DISTINCT(user_pseudo_id)) as users
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'add_to_cart'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') = 'https://shop.googlemerchandisestore.com/Google+Redesign/Apparel'
```

## Count on session_id on filtered event name + filtered event_params
```sql
SELECT
  COUNT(DISTINCT(CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),user_pseudo_id))) as sessions
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'add_to_cart'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') = 'https://shop.googlemerchandisestore.com/Google+Redesign/Apparel'
```

## Calculation on a param - Mean
```sql
SELECT
  AVG((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value')) AS mean
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'
```

## Calculation on a param - Quartiles
```sql
SELECT
  APPROX_QUANTILES((SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'),4) AS quartiles
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'
```

## 2 - Dimension designer

## Basic BQ field selection
```sql
SELECT
  event_name,
  count(*) as hits
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY event_name
ORDER BY hits desc
```

## BQ field with lookup table
```sql
SELECT
  CASE  
    WHEN event_name IN ('view_item','view_promotion','add_to_cart','begin_checkout','select_item') THEN 'E-commerce'
    WHEN event_name IN ('session_start','first_visit','user_engagement') THEN 'Automatic'
    ELSE 'Custom' 
    END
  as event_name_reclassified,
  count(*) as hits
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY event_name_reclassified
ORDER BY hits desc
```

## 3 - Report Builder

## Basic combination of 1 and 2
```sql
WITH p1 as(
SELECT
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'promotion_name') as promotion_name,
  (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') as value,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'
GROUP BY all),

p2 as(
SELECT
  CASE  
    WHEN promotion_name IN ('complete your collection','act responsible') THEN 'Promo Summer 2023'
    WHEN promotion_name IN ('google mural collection','reach new heights') THEN 'Promo Summer 2024' 
    ELSE 'Other'
    END
  as promo_reclassified,
  avg(value) as average_basket,
  count(*) as count_of_transactions
FROM p1
GROUP BY promo_reclassified)

SELECT
  *
FROM p2
WHERE count_of_transactions > 500
LIMIT 2
```

## 4 - Result screen

## Basic query 2 metrics
```sql
SELECT
  device.web_info.browser,
  geo.country,
  count(distinct(user_pseudo_id)) as events,
  count(distinct(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_ssession_id')) as sessions,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY all

## Basic query 2 metrics and calculated field
WITH p1 as(
SELECT
  device.web_info.browser,
  geo.country,
  count(distinct(user_pseudo_id)) as events,
  count(distinct(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) as sessions,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY all)

SELECT 
  *,
  p1.events / p1.sessions as events_per_sessions
FROM p1
```

## 3.5 Segments logic

## Session segment
```sql
WITH P1 as(
SELECT
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'view_promotion'),

p2 as(
SELECT
  traffic_source.source,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE 
  event_name = 'purchase')

SELECT
  source, 
  count(*) as purchases
FROM
  p1 left join p2 on p1.session_id = p2.session_id
WHERE
  p2.session_id is not null
GROUP BY all
```

## User segment 
```sql
WITH P1 as(
SELECT
  user_pseudo_id
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'view_promotion'),

p2 as(
SELECT
  traffic_source.source,
  user_pseudo_id
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE 
  event_name = 'purchase')

SELECT
  source, 
  count(*) as purchases
FROM
  p1 left join p2 on p1.user_pseudo_id = p2.user_pseudo_id
WHERE
  p2.user_pseudo_id is not null
GROUP BY all
```

## 4 - Realtime report builder
```sql
SELECT 
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page,
  COUNT(CASE 
          WHEN event_timestamp >= UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)) THEN 1 
        END) as pv_last_5_minutes,
  COUNT(CASE 
          WHEN event_timestamp >= UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)) 
               AND event_timestamp < UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)) THEN 1 
        END) as pv_5_to_10_minutes,
  COUNT(CASE 
          WHEN event_timestamp >= UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)) 
               AND event_timestamp < UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)) THEN 1 
        END) as pv_10_to_15_minutes
FROM
  `marketing-data-251112.analytics_320443981.events_intraday_20240609`
  -- A remplacer par une table intraday sur laquelle vous avez des accès 
WHERE
  event_name = 'page_view'
GROUP BY page
ORDER BY pv_last_5_minutes DESC
LIMIT 10
```

## 5 - Time calculator
```sql
WITH p1 as(
SELECT
  user_pseudo_id,
  event_timestamp,
  event_name,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page,
  LEAD(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) as next_timestamp
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE 
  event_name IN ('page_view','user_engagement') 
ORDER BY event_timestamp ASC),

p2 as(
SELECT
  page,
  next_timestamp - event_timestamp  as diff
FROM p1)

SELECT
  page,avg(diff)/1e-6 as avg_time_spent
FROM p2
GROUP BY all
ORDER by avg_time_spent desc
```

## 6 - Distribution analysis
```sql
with p1 as(
select 
  user_pseudo_id,
  event_name,
  CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),user_pseudo_id) as session_id
from
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where event_name in ('page_view','user_engagement')
),

p2 as(
select
  session_id,
  count(*) as hits
from p1
group by session_id),

p3 as(
select
  hits,
  count(*) as nb_hits
from p2
group by hits 
order by hits asc)

select
    CASE
    WHEN hits BETWEEN 1 AND 1 THEN 'Bounce'
    WHEN hits BETWEEN 2 AND 5 THEN '2 to 5'
    ELSE '5+' 
  END AS session_bucket,
  SUM(nb_hits) as nb_hits
from p3
group by session_bucket
order by nb_hits desc
```

## 7 - Value analysis
```sql
WITH P1 AS(
SELECT
  user_pseudo_id,
  event_timestamp, 
  event_name,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page,
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name IN ('page_view','purchase') AND user_pseudo_id is not null
ORDER BY event_timestamp asc),

p2 as(
SELECT
  user_pseudo_id,
  event_timestamp as conversion_timestamp
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'purchase' AND user_pseudo_id is not null
),

p3 as(
select
  p1.user_pseudo_id,
  p1.event_name,
  p1.event_timestamp,
  p1.page,
  p2.conversion_timestamp
FROM
  p1 full join p2 on p1.user_pseudo_id = p2.user_pseudo_id
WHERE p1.event_timestamp < conversion_timestamp),

p4 as(
SELECT 
  user_pseudo_id,
  count(*) as hits_by_user
from p3
group by user_pseudo_id
),

p5 as(
SELECT
  *
FROM
  p3 full join p4 on p3.user_pseudo_id = p4.user_pseudo_id)

SELECT
  page,
  SUM(100/p5.hits_by_user) as weight,
FROM 
  p5
GROUP BY page
ORDER BY weight desc
```
