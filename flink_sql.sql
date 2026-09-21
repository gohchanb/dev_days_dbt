CREATE TEMPORARY TABLE `public_user_info` (
  `id` INT,
  `user_name` STRING,
  `external_id` STRING,
  `last_login_timestamp` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT,
  `event_time` AS TO_TIMESTAMP_LTZ(`__event_time`, 3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'vcjffksb-default.public.user_info.A6sNbSbu',
  'properties.bootstrap.servers' = 'b-1-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198,b-2-public.etleaptestmsk.id7hbd.c4.kafka.us-east-1.amazonaws.com:9198',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'AWS_MSK_IAM',
  'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::959096951266:role/etleap-account-role" awsStsRegion="us-east-1";',
  'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)

CREATE TABLE IF NOT EXISTS `public_user_info_flink_sql` (
  `id` INT,
  `user_name` STRING,
  `external_id` STRING,
  `last_login_timestamp` TIMESTAMP(3),
  `__op` STRING,
  `__seq` BIGINT,
  `__event_time` BIGINT
);

INSERT INTO `public_user_info_flink_sql`
SELECT
  `id`,
  `user_name`,
  `external_id`,
  `last_login_timestamp`,
  `__op`,
  `__seq`,
  `__event_time`
FROM `public_user_info`;
