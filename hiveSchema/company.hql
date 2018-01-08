DROP TABLE company;
CREATE EXTERNAL TABLE company (
	name string,
	size decimal,
	location string
)
COMMENT "The table [company]"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\,'
LOCATION '/tmp';
