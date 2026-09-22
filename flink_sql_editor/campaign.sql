CREATE TEMPORARY TABLE `public_ad_impressions` (
  `impression_id` BIGINT,
  `ad_id` STRING,
  `campaign_id` STRING,
  `country` STRING,
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.ad_impressions.hjPQEanj',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);



CREATE TEMPORARY TABLE `public_ad_clicks` (
  `click_id` BIGINT,
  `impression_id` BIGINT,
  `campaign_id` STRING,
  `event_time` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'zgxa9tdp-default.public.ad_clicks.QUjx8xhc',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS `campaign_ctr` (
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `campaign_id` STRING,
  `impressions` BIGINT,
  `clicks` BIGINT,
  `ctr` DOUBLE
);

INSERT INTO `campaign_ctr`
SELECT
  TUMBLE_START(`event_time`, INTERVAL '1' MINUTE) AS `window_start`,
  TUMBLE_END(`event_time`, INTERVAL '1' MINUTE)   AS `window_end`,
  `campaign_id`,
  COUNT(DISTINCT `impression_id`) FILTER (WHERE `kind` = 'impression') AS `impressions`,
  COUNT(*)                        FILTER (WHERE `kind` = 'click')      AS `clicks`,
  CAST(COUNT(*) FILTER (WHERE `kind` = 'click') AS DOUBLE)
    / NULLIF(COUNT(DISTINCT `impression_id`) FILTER (WHERE `kind` = 'impression'), 0) AS `ctr`
FROM (
  SELECT `campaign_id`, `event_time`, `impression_id`, 'impression' AS `kind` FROM `public_ad_impressions`
  UNION ALL
  SELECT `campaign_id`, `event_time`, `impression_id`, 'click' AS `kind` FROM `public_ad_clicks`
)
GROUP BY `campaign_id`, TUMBLE(`event_time`, INTERVAL '1' MINUTE);