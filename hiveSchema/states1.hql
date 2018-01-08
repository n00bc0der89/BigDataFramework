DROP TABLE IF EXISTS states1;
CREATE EXTERNAL TABLE states1 (
  state struct<state_id:string, state_name:string>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/hduser/bigdataframework/hiveSchema/json_schema';
