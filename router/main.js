let sftp = require('./sftpscriptbuilder');
let fs = require("fs");
let execSync = require('child_process').execSync;
let exec = require('child_process').exec;
let Q = require("q");
let filepath = "";
const logger = require('../logs/log.js');
const config = require('../config.js');
let multer = require('multer');
let twitterstreaming = require('./twitterStreaming');

let Storage = multer.diskStorage({
	destination: function(req, file, callback) {
		callback(null, __dirname + "/../uploads");
	},
	filename: function(req, file, callback) {
		callback(null, file.originalname);
		// callback(null, file.fieldname + "_" + Date.now() + "_" + file.originalname);
	}
});

let upload = multer({ storage: Storage }).array("fileUploader", 3);

module.exports = function(app) {

	app.get("/", function(req, res) {
		console.log("From INdex");
		var data = { "createSqoopJobEndpoint": config.createSqoopJobEndpoint };
		res.render("index", data);
	});

	app.get("/sftp", function(req, res) {
		var data = { "Response": "" };
		res.render("sftp", data);
	});

	app.get("/hiveSchema", function(req, res) {
		var data = { "Response": "" };
		res.render("hiveSchema", data);
	});

	app.post("/hiveSchemaLogic", function(req, res) {

		upload(req, res, function(err) {
			if (err) {
				console.log('err', err);
				return res.end("Something went wrong!", err);
			}
			else {
				console.log(req.files[0].originalname, "File has been uploaded");
				console.log('Hive scehma create request body ', req.body);

				var inputFileType = req.body.inputFileType;
				var hdfsPath = req.body.hdfsPath;
				var storeAsFormat = req.body.storeAsFormat;
				var isCreateHiveTable = req.body.isCreateHiveTable;
				// var hiveDatabase = req.body.hiveDatabase;
				var hiveTable = req.body.hiveTable;

				var inputFileLocation = req.files[0].path;
				var originalFileName = req.files[0].originalname;
				originalFileName = originalFileName.toString().substring(0, originalFileName.toString().indexOf('.csv'));

				// req.files.forEach(function(f) {
				// 	console.log(f);
				// });

				if (inputFileType == 'csv') {
					console.log('Creating hive schema for CSV file');
					let command;

					if (isCreateHiveTable == 'yes') {
						command = __dirname + '/../scripts/Csv2Hive/bin/csv2hive.sh --create --table-name ' + hiveTable + ' ' + inputFileLocation + ' ' + __dirname + '/../hiveSchema' + ' ' + hdfsPath;
						console.log('command -> ', command);

						exec(command, function(error, stdout, stderr) {
							// console.log('err>>>>>>>>>>', error);
							// console.log('stdout>>>>>>>>>>', stdout);
							// console.log('stderr>>>>>>>>>>', stderr);
							var data = {"Response" : "Hive table has been created. \nTable `" + hiveTable + "` schema has been generated at - " + __dirname + "/../hiveSchema/" + hiveTable + ".hql"}
							res.render("hiveSchema",data);
						});
					}
					else {
						command = __dirname + '/../scripts/Csv2Hive/bin/csv2hive.sh --table-name ' + hiveTable + ' ' + inputFileLocation + ' ' + __dirname + '/../hiveSchema' + ' ' + hdfsPath;
						console.log('command -> ', command);

						exec(command, function(error, stdout, stderr) {
							// console.log('err>>>>>>>>>>', error);
							// console.log('stdout>>>>>>>>>>', stdout);
							// console.log('stderr>>>>>>>>>>', stderr);
							var data = {"Response" : "Table `" + hiveTable + "` schema has been generated at - " + __dirname + "/../hiveSchema/" + originalFileName + ".hql"}	
							res.render("hiveSchema",data);
						});
					}
				}
				else if (inputFileType == 'json') {
					console.log('Creating hive schema for JSON file');
					let command;
					
					if (isCreateHiveTable == 'yes') {
						command = __dirname + '/../scripts/Json2Hive/json2hive.sh ' + inputFileLocation + ' ' + hdfsPath + ' ' + hiveTable + ' 1' ;
						console.log('command -> ', command);
	
						exec(command, function(error, stdout, stderr) {
							// console.log('err>>>>>>>>>>', error);
							// console.log('stdout>>>>>>>>>>', stdout);
							// console.log('stderr>>>>>>>>>>', stderr);
	
							var data = {"Response" : "Hive table has been created. \nTable `" + hiveTable + "` schema has been generated at - " + __dirname + "/../hiveSchema/" + hiveTable + ".hql"}
                                                        res.render("hiveSchema",data);
						});
						
					} else {
						command = __dirname + '/../scripts/Json2Hive/json2hive.sh ' + inputFileLocation + ' ' + hdfsPath + ' ' + hiveTable + ' 0';
						console.log('command -> ', command);
	
						exec(command, function(error, stdout, stderr) {
							// console.log('err>>>>>>>>>>', error);
							// console.log('stdout>>>>>>>>>>', stdout);
							// console.log('stderr>>>>>>>>>>', stderr);
	
							var data = {"Response" : "Table `" + hiveTable + "` schema has been generated at - " + __dirname + "/../hiveSchema/" + hiveTable + ".hql"}
                                                        res.render("hiveSchema",data);
						});
					}
					
				}
			}
		});
	});

	app.post("/sftpLogic", function(req, res) {
		//console.log("From Sftp");
		logger.log("From Sftp");
		//var data = { "Response" : ""};

		let hostname = req.body.host;
		let username = req.body.username;
		let path = req.body.path;
		let hdfspath = req.body.hdfspath;
		let mode = req.body.mode;
		let startdate = req.body.startDateTime;
		let frequency = req.body.frequencyInMinutes;

		let filename = "output_" + new Date().getTime().toString() + ".sh";
		filepath = __basedir + "/sftp_scripts/" + filename;
		//console.log("Filepath : " + filepath);
		let obj = { host: hostname, username: username, path: path, hdfspath: hdfspath }
		//let r = sftp.sftpCore(obj);
		let scripts = sftp.sftpScriptBuilder(obj);
		//console.log(scripts);
		if (scripts != null) {
			//console.log("Writing shellscript... ");
			logger.info("Writing shellscript... ");
			fs.writeFileSync(filepath, scripts, 'utf-8');
		}

		// Change permission of the script and scheduled it
		//console.log("Assigning permission to shellscript");
		logger.info("Assigning permission to shellscript ");
		let chmodcommand = "chmod 777 " + filepath;
		execSync(chmodcommand);

		//let runjob = "sh " + filepath;
		//execSync(runjob);

		//Schedule Oozie workflow
		//console.log("Calling Oozie Flow function");
		logger.info("Calling Oozie Flow function ");
		let status = sftp.runOozieFlow(mode, filename, filepath, startdate, frequency);
		//console.log(status);
		var data = { "Response": status }
		res.send(data);
	});


	app.get("/twitterstream",function(req,res){

			res.render("twitterstream");

	});

	app.post("/TwitterStreamLogic",function(req,res){
	console.log("inside logic")
	logger.info("Twitter Streaming Started");

	// Set Property files for streaming
	let consumer_key = req.body.c_key;
	let secret_key = req.body.s_key;
	let access_token = req.body.a_token;
	let secret_access_token = req.body.as_token;
	let keyword = req.body.kw;

	let obj = {  "consumer_key" : consumer_key , "secret_key" : secret_key, "access_token" : access_token , "secret_access_token" : secret_access_token,
	"keyword" : keyword}

	//Create twitter properties.
	console.log(obj);
	twitterstreaming.createTwitterProperties(obj);
	twitterstreaming.createConfProperties(obj);


	logger.info("Twitter Streaming End");

	});


}
