CREATE EXTERNAL TABLE `accelerometer_landing`(
	`user` string COMMENT 'from deserializer',
	`timestamp` bigint COMMENT 'from deserializer',
	`x` double COMMENT 'from deserializer',
	`y` double COMMENT 'from deserializer',
	`z` double COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tomasz-project-stedi-lake-house/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json')