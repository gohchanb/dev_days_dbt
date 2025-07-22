{{ config(
    pre_hook=[
        "ALTER ICEBERG TABLE PUBLIC.\"ORDERS_KqRs6Yo0\" REFRESH",
        "set MIN_DATE = dateadd(minute, -5, current_timestamp())"
    ]
) }}

select
  account_id,
  symbol,
  sum(qty)                                   as shares_submitted_10m,
  sum(filled_qty)                            as shares_filled_10m,
  count_if(status = 'CANCELED')              as cancels_10m,
  sum(
    case when status = 'OPEN' 
      then pnl_usd 
      else 0 
    end) as pnl_risk_usd,
  max(updated_at)                            as last_update
from {{ source('PUBLIC', 'ORDERS') }}
group by account_id, symbol
