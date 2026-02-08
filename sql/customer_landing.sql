CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  customerName              string,
  email                     string,
  phone                     string,
  birthDay                  string,
  serialNumber              string,
  registrationDate          bigint,
  lastUpdateDate            bigint,
  shareWithResearchAsOfDate bigint,
  shareWithPublicAsOfDate   bigint,
  shareWithFriendsAsOfDate  bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://christina-stedi-datalake/landing/customer/'
TBLPROPERTIES ('has_encrypted_data'='false');
