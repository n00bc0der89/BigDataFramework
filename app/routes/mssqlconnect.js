//Add MySQL Jar File Check

'use strict';

const sql = require('mssql');
const exec = require('child_process').exec;
const config = require('../../config.js');

module.exports = function (app, express) {
    var api = express.Router();

	api.post('/mssqlcreatesqoopjob', function(req,res) {
		
		let hostname = req.body.host;
		let port = req.body.port || 1433;
		let username = req.body.username;
		let password = req.body.password;
		let database = req.body.database;
		let dbType = req.body.type;
		let dbTable = req.body.table;
		let hdfsPath = req.body.hdfspath;
		let sqoopJobName = dbType + "_" + database + "_" + dbTable;
		let connectionString;
		if(dbType.toLowerCase() === "mysql"){
			connectionString = "jdbc:mysql://"+hostname+":"+port+"/"+database;  
		} else if (dbType.toLowerCase() === "mssql")  {
			connectionString = "jdbc:sqlserver://"+hostname+":"+port+";databaseName="+database;
		}
		
		// Sqoop Job Delete Command Preparation
		let sqoop_job_delete = "sqoop job --delete "+sqoopJobName;
		
		//Sqoop Job Creation Command
		let sqoop_job_create = "sqoop job --create "+sqoopJobName+" -- import --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --username "+username+" --password "+password+" --table "+dbTable+" --m 1 --target-dir " + config.hadoopBasePath + "employee/ --append --connect "+connectionString;
		console.log(sqoop_job_create);
		let sjd = exec(sqoop_job_delete, function (error, stdout, stderr) {
			if(error !== null){
				console.log("error occured : " + error);
			} else {
				console.log("Sqoop Job Deleted successfully");
				let sjd = exec(sqoop_job_create, function (error, stdout, stderr) {
					if(error !== null){
						console.log("error occured : " + error);
						res.json({"message":"Error occured on sqoop job creation","statusCode":"201"});
					} else {
						let hdfs_path_create = "hadoop dfs -mkdir "+hdfsPath+"/"+sqoopJobName;
						let hpc = exec(hdfs_path_create, function (error, stdout, stderr) {
							if(error !== null){
								console.log("error occured : " + error);
								res.json({"message":"Hadoop Directory already exists.","statusCode":"200"});
							} else {
								res.json({"message":"Hadoop Directory Successfully Created","statusCode":"200"});
							}
						});
					}
				});
			}
		});
	});

    api.post('/mssqlprerequisitecheck', function (req, res) {

        let hostname = req.body.host;
        let port = req.body.port || 1433;
        let username = req.body.username;
        let password = req.body.password;
        let database = req.body.database;

    	let config = {
        	user: username,
        	password: password,
        	server: hostname, 
        	database: database 
    	};

        sql.connect(config,function(err) {
		sql.close();
		if (err) {
			console.log(err);
            		res.json({"message":"Connection Error","code":err.code,"desc":err.sqlMessage,"fatal":err.fatal,"statusCode":"201"})
            		return;
          	}

                let child = exec("sqoop version", function (error, stdout, stderr) {
                 	if (error !== null) {
                        	console.log('exec error: ' + error);
                        } else {
                        	console.log(stdout);
				if(stdout.toString().indexOf("command not found") === -1){
					res.json({"message":"Microsoft SQL Server Connection Successfull & Sqoop Exists","statusCode":"200"});
				}else{
					res.json({"message":"Microsoft SQL Server Connection Successfull but Sqoop does not exist","statusCode":"202"})
				}
                        }
                });
        });

    });

    return api;
};
