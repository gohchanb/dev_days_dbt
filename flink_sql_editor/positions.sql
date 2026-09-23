CREATE TEMPORARY TABLE `public_trades` (
  `trade_id` BIGINT,
  `account_id` STRING,
  `symbol` STRING,
  `side` STRING,
  `quantity` INT,
  `price` DECIMAL(38, 18),
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.trades.15URt9pI',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE `public_ticker_prices` (
  `tick_id` BIGINT,
  `symbol` STRING,
  `price` DECIMAL(38, 18),
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.ticker_prices.j2KCWOx5',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY VIEW `ticker_prices_versioned` AS
SELECT `symbol`, `price`, `event_time`
FROM (
  SELECT
    `symbol`,
    `price`,
    `event_time`,
    ROW_NUMBER() OVER (PARTITION BY `symbol` ORDER BY `event_time` DESC) AS `rn`
  FROM `public_ticker_prices`
)
WHERE `rn` = 1;

CREATE TABLE IF NOT EXISTS `cash_position` (
  `account_id` STRING,
  `event_time` TIMESTAMP(3),
  `trade_id` BIGINT,
  `cash_balance` DOUBLE
);

CREATE TABLE IF NOT EXISTS `asset_position` (
  `account_id` STRING,
  `symbol` STRING,
  `event_time` TIMESTAMP(3),
  `trade_id` BIGINT,
  `net_quantity` BIGINT,
  `market_value` DOUBLE
);

INSERT INTO `cash_position`
SELECT
  `account_id`,
  `event_time`,
  `trade_id`,
  SUM(`signed_amount`) OVER (
    PARTITION BY `account_id`
    ORDER BY `event_time`
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS `cash_balance`
FROM (
  SELECT
    `account_id`,
    `event_time`,
    `trade_id`,
    (CASE `side` WHEN 'BUY' THEN -1.0 ELSE 1.0 END)
      * CAST(`quantity` AS DOUBLE) * CAST(`price` AS DOUBLE) AS `signed_amount`
  FROM `public_trades`
);

INSERT INTO `asset_position`
SELECT
  `account_id`,
  `symbol`,
  `event_time`,
  `trade_id`,
  `net_quantity`,
  CAST(`net_quantity` AS DOUBLE) * `price_at_event` AS `market_value`
FROM (
  SELECT
    e.`account_id`,
    e.`symbol`,
    e.`event_time`,
    e.`trade_id`,
    e.`price_at_event`,
    SUM(e.`signed_qty`) OVER (
      PARTITION BY e.`account_id`, e.`symbol`
      ORDER BY e.`event_time`
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS `net_quantity`
  FROM (
    SELECT
      t.`account_id`,
      t.`symbol`,
      t.`event_time`,
      t.`trade_id`,
      CASE t.`side` WHEN 'BUY' THEN t.`quantity` ELSE -t.`quantity` END AS `signed_qty`,
      CAST(tp.`price` AS DOUBLE) AS `price_at_event`
    FROM `public_trades` t
    JOIN `ticker_prices_versioned` FOR SYSTEM_TIME AS OF t.`event_time` AS tp
      ON t.`symbol` = tp.`symbol`
  ) e
);