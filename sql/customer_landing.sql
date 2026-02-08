CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  customername              string,
  email                     string,
  phone                     string,
  birthday                  string,
  serialnumber              string,
  registrationdate          bigint,
  lastupdatedate            bigint,
  sharewithresearchasofdate bigint,
  sharewithpublicasofdate   bigint,
  sharewithfriendsasofdate  bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://christina-stedi-datalake/landing/customer/'
TBLPROPERTIES ('has_encrypted_data'='false');

