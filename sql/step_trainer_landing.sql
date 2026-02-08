CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorreadingtime   string,
  serialnumber        string, 
  distancefromobject  double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://christina-stedi-datalake/landing/step_trainer/'
TBLPROPERTIES ('has_encrypted_data'='false');

