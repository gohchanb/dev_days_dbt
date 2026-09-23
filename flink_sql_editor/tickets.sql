CREATE TEMPORARY TABLE `public_ticket_reservations` (
  `reservation_id` BIGINT,
  `event_id` STRING,
  `ticket_type` STRING,
  `quantity` INT,
  `user_id` STRING,
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.ticket_reservations.3ctL0jxk',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE `public_ticket_purchases` (
  `purchase_id` BIGINT,
  `reservation_id` BIGINT,
  `amount` DECIMAL(38, 18),
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.ticket_purchases.suTEWJDg',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS `confirmed_sales` (
  `reservation_id` BIGINT, `event_id` STRING, `ticket_type` STRING,
  `quantity` INT, `amount` DECIMAL(12, 2), `purchased_at` TIMESTAMP_LTZ(3)
);

INSERT INTO `confirmed_sales`
SELECT r.`reservation_id`, r.`event_id`, r.`ticket_type`, r.`quantity`,
       p.`amount`, p.`event_time` AS purchased_at
FROM public_ticket_reservations r
JOIN public_ticket_purchases p
  ON r.`reservation_id` = p.`reservation_id`
 AND p.`event_time` BETWEEN r.`event_time` AND r.`event_time` + INTERVAL '30' MINUTE;

CREATE TABLE IF NOT EXISTS `released_tickets` (
  `reservation_id` BIGINT, `event_id` STRING, `ticket_type` STRING,
  `quantity` INT, `user_id` STRING, `reserved_at` TIMESTAMP_LTZ(3)
);

INSERT INTO `released_tickets`
SELECT r.`reservation_id`, r.`event_id`, r.`ticket_type`, r.`quantity`, r.`user_id`,
       r.`event_time` AS reserved_at
FROM public_ticket_reservations r
LEFT JOIN public_ticket_purchases p
  ON r.`reservation_id` = p.`reservation_id`
 AND p.`event_time` BETWEEN r.`event_time` AND r.`event_time` + INTERVAL '30' MINUTE
WHERE p.`reservation_id` IS NULL;