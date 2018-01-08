DROP TABLE orders;
CREATE EXTERNAL TABLE orders (
	qCode string,
	indCode string,
	startDate string,
	endDate boolean
)
COMMENT "The table [orders]"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\,'
LOCATION '/user/hduser/demo/csv';
