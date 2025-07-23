SELECT p.user_id, SUM(i.price) AS spend
  FROM {{source('DBTDEMO', 'PURCHASE')}} p
    INNER JOIN {{source('DBTDEMO', 'LINE_ITEM')}} li ON li.purchase_id = p.id
    INNER JOIN {{source('DBTDEMO', 'ITEM')}} i ON i.item_id = li.item_id
    -- INNER JOIN DBTDEMO.USER u ON u.id = p.user_id
  GROUP BY p.user_id
