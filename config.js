// Not using this class for now.. Only use MYSQL.
module.exports = {
  "database":"mongodb://root:abc123@dbh70.mongolab.com:27707/userstory",
  "port":process.env.PORT || 3010,
  "secretKey" : "123456789",
  
  "distribution" : "Cloudera",
  
  "nameNodeHost" : "ip-172-31-17-159.us-west-2.compute.internal",
  "nameNodePort" : 8020,
  "jobTrackerHost" : "ip-172-31-17-159.us-west-2.compute.internal",
  "jobTrackerPort" : 8032,
  
  "localBasePath" : "/home/hduser/BigDataFramework/jobs/",
  "hadoopBasePath" : "/user/hduser/bigdataframework/jobs/",

  "logLevel" : "debug",
  "logDir" : "/home/hduser/BigDataFramework/logs",
  "logEnvironment" : "development",
  
  "mySQL" : "mysql",
  "msSQL" : "mssql",
  "nullValue" : null,
  
  "ooziejob" : { "Sqoop" : "sqoop" , "Cron" : "cron"},
  "oozieHome": "/opt/cloudera/parcels/CDH/lib/oozie/",
  "oozieWebHost": "http://172.31.17.159:11000/oozie",
  "sqoopMetaStoreHost" : "172.31.17.159",
  "sqoopMetaStorePort" : 16000,
  "createSqoopJobEndpoint" : "52.10.199.79:3010/bigdataframework/api/createsqoopjob",
  
  "mysqlHost":"",
  "mysqlPort" : 3306,
  "mysqlUser":"",
  "mysqlPassword":""
};
