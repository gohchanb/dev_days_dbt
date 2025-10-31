SELECT u.id AS user_id, COUNT(e.external_id) AS logins 
FROM {{source('benedict_test', 'user_info_test3')}} u, {{source('benedict_test', 'WEB_EVENTS')}} e
WHERE u.external_id = e.external_id
GROUP BY u.id
