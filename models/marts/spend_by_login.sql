{{ config(materialized='view') }}

SELECT s.user_id, SUM(s.spend)/SUM(l.logins) AS spend_by_login 
FROM {{ref('spend_per_user')}} s
  INNER JOIN {{ref('logins_by_user')}} l ON l.user_id = s.user_id
GROUP BY s.user_id
ORDER BY spend_by_login desc