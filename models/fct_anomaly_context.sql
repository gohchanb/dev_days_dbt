select
  symbol_order.account_id,
  symbol_order.symbol,
  symbol_order.pnl_risk_usd as open_exposure_usd,
  watchlist_anomaly.account_name,
  watchlist_anomaly.anomaly_create_date,
  watchlist_anomaly.watch_reason,
  watchlist_anomaly.watchlist_date,
  watchlist_anomaly.anomaly_reason,
  watchlist_anomaly.anomaly_severity
from {{ ref('int_symbol_order_5min') }} symbol_order
left join {{ ref('int_watchlist_anomaly') }} watchlist_anomaly
  on symbol_order.account_id = watchlist_anomaly.account_id
  and symbol_order.symbol = watchlist_anomaly.symbol
  and watchlist_anomaly.anomaly_create_date > dateadd(minute, -5, current_timestamp())
SELECT s.user_id, SUM(s.spend)/SUM(l.logins) AS spend_by_login 
FROM {{ref('spend_per_user')}} s
  INNER JOIN {{ref('logins_by_user')}} l ON l.user_id = s.user_id
GROUP BY s.user_id
ORDER BY spend_by_login desc