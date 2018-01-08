let fs = require("fs");
//let script_string = "";
let endlinecharacter = "\n";
let config = require('../config');
let common = require('../common');
const exec = require('child_process').exec;
const path = require('path');
const logger = require('../logs/log.js');

module.exports.sftpScriptBuilder = function(obj)
{
let script_string = "";
let host = obj.host;  //"172.16.243.116";
let username = obj.username; //"mapr";
let logpathdirectory = obj.path; //"/home/mapr/logs/error.log";
let hdfs_path = obj.hdfspath;

// Build the shellscript and run it.
//script_string += "exec &> " + config.logDir + "/" + "shellscriptrun.log";
//script_string += endlinecharacter;
script_string += "local_temp_folder=\"/tmp/staging_sftp\"";
script_string += endlinecharacter;
script_string += "remote_log_path=\"" + logpathdirectory + "\"";
script_string += endlinecharacter;
script_string += "remote_machine=\"" + host + "\"";
script_string += endlinecharacter;
script_string += "remote_user=\"" + username + "\"";
script_string += endlinecharacter;
script_string += "hdfs_folder_path=\"" + hdfs_path + "\"";
script_string += endlinecharacter;
script_string += "home_path=\"/home/\"$remote_user";
script_string += endlinecharacter;
script_string += "# Make local temp directory to store the remote files.";
script_string += endlinecharacter;
script_string += "echo \"Checking if tmp staging folder exists?\"";
script_string += endlinecharacter;
script_string += "echo $local_temp_folder";
script_string += endlinecharacter;
script_string += "if [ -d $local_temp_folder ]";
script_string += endlinecharacter;
script_string += "then";
script_string += endlinecharacter;
script_string += "echo \"Temp staging Directory already  exists. Hence removing.\"";
script_string += endlinecharacter;
script_string += "rm -rf $local_temp_folder";
script_string += endlinecharacter;
script_string += "fi";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "# Creating Temp staging local directory";
script_string += endlinecharacter;
script_string += "echo \"Creating temp local directory\"";
script_string += endlinecharacter;
script_string += "mkdir $local_temp_folder";
script_string += endlinecharacter;
script_string += "echo \"temp local directory created\"";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "# Make secure copy of file from remote server.";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "echo \"Securely copying data from Remote Hosts\"";
script_string += endlinecharacter;
script_string += "scp -r $remote_user@$remote_machine:$remote_log_path/* $local_temp_folder";
script_string += endlinecharacter;
script_string += "echo \"Copying data complete ...\"";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "#Push Files into HDFS";
script_string += endlinecharacter;
script_string += "echo \"Pushing files into HDFS\"";
script_string += endlinecharacter;
script_string += "hadoop fs -put $local_temp_folder/* $hdfs_folder_path";
script_string += endlinecharacter;
script_string += "echo \"Files pushed into HDFS\"";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "# Move Remote file to Done directory";
script_string += endlinecharacter;
script_string += "echo \"Moving Remote files to Done directory\"";
script_string += endlinecharacter;
script_string += "ssh $remote_user@$remote_machine << 'ENDSSH'";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "remote_user=\"" + username + "\"";
script_string += endlinecharacter;
script_string += "hdfs_folder_path=\"" + hdfs_path + "\"";
script_string += endlinecharacter;
script_string += "home_path=\"/home/\"$remote_user";
script_string += endlinecharacter;
script_string += "remote_log_path=\"" + logpathdirectory + "\"";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "echo \"Homepath : $home_path\"";
script_string += endlinecharacter;
script_string += "if  [ ! -d \"$home_path/Done\" ]";
script_string += endlinecharacter;
script_string += "then";
script_string += endlinecharacter;
script_string += "echo \"Creating Done Directory\"";
script_string += endlinecharacter;
script_string += "mkdir \"$home_path/Done\"";
script_string += endlinecharacter;
script_string += "fi";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "mv $remote_log_path/* $home_path/Done";
script_string += endlinecharacter;
script_string += endlinecharacter;
script_string += "echo \"Remote Files moved to Done directory\"";
script_string += endlinecharacter;
script_string += "exit;";
script_string += endlinecharacter;
script_string += "ENDSSH";
return script_string;

}


module.exports.runOozieFlow = function(mode,filename,filepath,startDateTime,freqInMinutes)
{
	let jobname = "sftp_" + filename.split('.')[0];
	let status = "Job Scheduled Successfully!!";
	let hdfs_path_create = "hadoop dfs -mkdir "+config.hadoopBasePath+ jobname;
	console.log("Running Oozie Workflow");
	logger.info("Running Oozie Workflow");
	let hpc = exec(hdfs_path_create, function (error, stdout, stderr) {
		if(error === config.nullValue){

			let namenode,jobtracker,libpath,queuename,coordinatorpath,finalData,workflowpath,myscript,myscriptpath;

			if(config.distribution.toLowerCase() === "mapr"){
				namenode = "nameNode=maprfs://"+config.nameNodeHost+":"+config.nameNodePort+"\n";
				jobtracker = "jobTracker="+config.jobTrackerHost+":"+config.jobTrackerPort+"\n";
				libpath = "oozie.use.system.libpath=true"+"\n";
				queuename = "queueName=sftpexample"+"\n";
				myscript = "myscript=" + filename +  "\n";
				myscriptpath = "myscriptPath=" + config.hadoopBasePath+jobname+ "/" + filename  + "\n";
				if (mode === 'once'){
					workflowpath = "oozie.wf.application.path="+config.hadoopBasePath+jobname+"/workflow.xml" + "\n";
					finalData = namenode + jobtracker + queuename + libpath + workflowpath + myscript + myscriptpath; 
				} else if (mode === 'incremental'){
					coordinatorpath = "oozie.coord.application.path="+config.hadoopBasePath+jobname+"/coordinator.xml" + "\n";
					finalData = namenode + jobtracker + queuename + libpath + coordinatorpath + myscript + myscriptpath;
				}
			}
			else
			{
				namenode = "nameNode=hdfs://"+config.nameNodeHost+":"+config.nameNodePort+"\n";
					jobtracker = "jobTracker="+config.jobTrackerHost+":"+config.jobTrackerPort+"\n";
					libpath = "oozie.use.system.libpath=true"+"\n";
					queuename = "queueName=sftpexample"+"\n";
					myscript = "myscript=" + filename +  "\n";
					myscriptpath = "myscriptPath=" + config.hadoopBasePath+jobname+ "/" + filename  + "\n";
					if (mode === 'once'){
						workflowpath = "oozie.wf.application.path="+config.hadoopBasePath+jobname+"/workflow.xml" + "\n";
						finalData = namenode + jobtracker + queuename + libpath + workflowpath + myscript + myscriptpath; 
					} else if (mode === 'incremental'){
						coordinatorpath = "oozie.coord.application.path="+config.hadoopBasePath+jobname+"/coordinator.xml" + "\n";
						finalData = namenode + jobtracker + queuename + libpath + coordinatorpath + myscript + myscriptpath;
					}		
	
			};

			let dir = config.localBasePath+jobname;
			
			if(!fs.existsSync(dir)){
				fs.mkdirSync(path.resolve(dir));
			}

			// Create job.properties file 
			fs.writeFile(dir+"/job.properties", finalData, { flag: 'wx' }, function (err) {
				console.log("Writing job properties file");
				if (err) throw err;
			    	console.log("It's saved!");	

				let workflowXml = common.generateWorkFlowXML(null,config.ooziejob.Cron);
				let coordinatorXml;
				if (mode === 'incremental') {		
					coordinatorXml = common.generateCoordinatorXML(config.hadoopBasePath,jobname,startDateTime,freqInMinutes);
				}

				//Create workflow.xml and move it to HDFS
				fs.writeFile(dir+"/workflow.xml", workflowXml, { flag: 'wx' }, function (err) {
					console.log("Writing workflow file");
					if (err) throw err;
					if (mode === 'incremental'){
						fs.writeFile(dir+"/coordinator.xml", coordinatorXml, { flag: 'wx' }, function (err) {
							if (err) throw err;
							common.moveFileToHadoop(jobname);
							common.moveScriptToHadoop(jobname,filepath);
							common.submitOozieJob(jobname);
							console.log("Incremental Job Successfully submited");
							status = "Incremental Job Scheduled Successfully!!";
							return status;
							
						});
					} else {
						common.moveFileToHadoop(jobname);
						common.moveScriptToHadoop(jobname,filepath);
						common.submitOozieJob(jobname);
						console.log("Once Job Scheduled Successfully");
						status = "Once Job Scheduled Successfully!!";
						return status;
						
					}	
				});
			});
		} else {
			logger.info("error occured : " + error);
			console.log("error occured : " + error);
			status = "Hadoop Directory Already Exists";
			return status;
		}
	});


	return status;

}
