CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  timestamp string,
  user      string,
  x         double,
  y         double,
  z         double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://christina-stedi-datalake/landing/accelerometer/'
TBLPROPERTIES ('has_encrypted_data'='false');

