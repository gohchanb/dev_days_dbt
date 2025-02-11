SELECT p.user_id, SUM(i.price) AS spend
  FROM {{source('benedict_test', 'PURCHASE')}} p
    INNER JOIN {{source('benedict_test', 'LINE_ITEM')}} li ON li.purchase_id = p.id
    INNER JOIN {{source('benedict_test', 'ITEM')}} i ON i.item_id = li.item_id
    -- INNER JOIN benedict_test.USER u ON u.id = p.user_id
  GROUP BY p.user_id
