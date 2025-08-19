SELECT u.id AS user_id, COUNT(e.external_id) AS logins 
FROM {{source('DBTDEMO', 'USER_INFO')}} u, {{source('DBTDEMO', 'WEB_EVENTS')}} e
WHERE u.external_id = e.external_id and e.external_id > 100
GROUP BY u.id
