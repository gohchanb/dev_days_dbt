{{ config(
    pre_hook=[
        "ALTER ICEBERG TABLE PUBLIC.\"TRADE_ANOMALY_rCnLfXHS\" REFRESH"
    ]
) }}

select
  account.zenith_account_id__c as account_id,
  account.name as account_name,
  anomaly.symbol__c as symbol,
  anomaly.severity__c as anomaly_severity,
  anomaly.reason__c as anomaly_reason,
  anomaly.created_date as anomaly_create_date,
  watchlist.watchlist_date,
  watchlist.watch_reason
from {{ source('PUBLIC', 'TRADE_ANOMALY') }} anomaly
join {{ source('PUBLIC', 'SALESFORCE_ACCOUNT' )}} account
  on anomaly.account__c = account.id
join {{ source('PUBLIC', 'WATCHLIST') }} watchlist
  on watchlist.entity_name = account.name