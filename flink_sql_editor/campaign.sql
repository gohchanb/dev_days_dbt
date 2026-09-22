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
  `window_start` TIMESTAMP_LTZ(3), `window_end` TIMESTAMP_LTZ(3),
  `campaign_id` STRING, `impressions` BIGINT, `clicks` BIGINT, `ctr` DOUBLE
);

INSERT INTO `campaign_ctr`
SELECT
  TUMBLE_START(i.`event_time`, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_END(i.`event_time`, INTERVAL '1' MINUTE)   AS window_end,
  i.`campaign_id`,
  COUNT(DISTINCT i.`impression_id`) AS impressions,
  COUNT(c.`click_id`)               AS clicks,
  CAST(COUNT(c.`click_id`) AS DOUBLE) / COUNT(DISTINCT i.`impression_id`) AS ctr
FROM ad_impressions i
LEFT JOIN ad_clicks c
  ON i.`impression_id` = c.`impression_id`
 AND c.`event_time` BETWEEN i.`event_time` AND i.`event_time` + INTERVAL '1' MINUTE
GROUP BY i.`campaign_id`, TUMBLE(i.`event_time`, INTERVAL '1' MINUTE);