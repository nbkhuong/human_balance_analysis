CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`accelerometer_landing` (
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float,
  `user` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://human-balance/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');