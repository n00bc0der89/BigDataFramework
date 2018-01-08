DROP TABLE IF EXISTS states;
CREATE EXTERNAL TABLE states (
  state struct<state_id:string, state_name:string>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/hduser/demo/json';
