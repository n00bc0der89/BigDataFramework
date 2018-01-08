"use strict";

const logger = require('./logs/log.js');
const builder = require('xmlbuilder');
const config = require('./config.js');
const execSync = require('child_process').execSync;

var common = {};

(function(common){

	common.generateCoordinatorXML = function(hadoopWorkFlowFilePath,JobName,startDateTime,freqInMinutes){

        	let coordinatorPath = hadoopWorkFlowFilePath + JobName + '/workflow.xml';
		let frequency = "*/"+freqInMinutes+" * * * *";

        	let xml =  builder.create('coordinator-app',{ encoding: 'utf-8' })
        	.att('name','coordinator1')
        	.att('frequency',frequency)
        	.att('start',startDateTime)
        	.att('end','9999-12-31T02:00Z')
        	.att('timezone','GMT+0530')
        	.att('xmlns','uri:oozie:coordinator:0.1')
        	.ele('action')
          	  .ele('workflow')
            	    .ele('app-path',{},coordinatorPath).up()
          	  .up()
        	.up()
        	.end({ pretty: true});

        	return xml;
	};

	common.generateWorkFlowXML = function(command,type){

        	//let command = "job --meta-connect jdbc:hsqldb:hsql://"+sqoopMetaStoreHost+":"+sqoopMetaStorePort+"/sqoop --exec "+sqoopJobName+" -- --password "+dbPassword;
            let xml = "";
            if(type == config.ooziejob.Sqoop)
            {
                xml = builder.create('workflow-app',{ encoding: 'utf-8' })
                .att('name','Sqoop_Test')
                .att('xmlns', 'uri:oozie:workflow:0.5')
                .ele('start',{'to':'sqoop-9420'}).up()
                .ele('action',{'name':'sqoop-9420'})
                  .ele('sqoop',{'xmlns':'uri:oozie:sqoop-action:0.2'})
                    .ele('job-tracker',{},'${jobTracker}').up()
                    .ele('name-node',{},'${nameNode}').up()
                    .ele('command',{},command).up()
                  .up()
                  .ele('ok',{'to':'End'}).up()
                  .ele('error',{'to':'kill'}).up()
                .up()
                .ele('kill',{'name':'kill'})
                  .ele('message',{},'Sqoop failed, error message[${wf:errorMessage(wf:lastErrorNode())}]').up()
                .up()
                .ele('end',{'name':'End'})
                .end({ pretty: true});

            }
            else if(type == config.ooziejob.Cron)
            {
		 xml = builder.create('workflow-app',{ encoding: 'utf-8' }).att('name','sftp_test').att('xmlns', 'uri:oozie:workflow:0.5')
			.ele('start',{'to':'shell-node'}).up()
			.ele('action',{'name':'shell-node'})
	  		.ele('shell',{'xmlns':'uri:oozie:shell-action:0.2'})
			.ele('job-tracker',{},'${jobTracker}').up()
			.ele('name-node',{},'${nameNode}').up()
			.ele('configuration',{})
			.ele('property',{})
			.ele('name',{},'mapred.job.queue.name').up()
			.ele('value',{},'${queueName}').up().up().up()
			.ele('exec',{},'${myscript}').up()
			.ele('env-var',{},'HADOOP_USER_NAME=${wf:user()}').up()
			.ele('file',{},'${myscriptPath}').up().up()
	  		.ele('ok',{'to':'End'}).up()
	  		.ele('error',{'to':'kill'}).up()
			.up()
			.ele('kill',{'name':'kill'})
	  		.ele('message',{},'Sftp failed, error message[${wf:errorMessage(wf:lastErrorNode())}]').up()
			.up()
			.ele('end',{'name':'End'})
			.end({ pretty: true});  		

            }
        	

        	return xml;
	}

	common.moveFileToHadoop = function(JobName){
        logger.info("MoveFileToHadoop Function call start");
		//console.log("MoveFileToHadoop Function call start");
		let copyFilesToHadoop = "hadoop dfs -put "+config.localBasePath+JobName+"/*.xml "+config.hadoopBasePath+JobName;
		let moveStatus = execSync(copyFilesToHadoop);
		//console.log("File moved to HDFS");
        logger.info("File moved to HDFS");
		//console.log("Removing Config files from local");
        logger.info("Removing Config files from local");
		let removeLocalFiles = "rm -rf "+config.localBasePath+JobName+"/*.xml";
		let removeStatus = execSync(removeLocalFiles);
		//console.log("MoveFileToHadoop Function call end");
        logger.info("MoveFileToHadoop Function call end");	
	}

    common.moveScriptToHadoop = function(JobName,filepath)
    {
	//console.log("MoveScriptToHadoop Function call start");
    logger.info("MoveScriptToHadoop Function call start");
        let copyFilesToHadoop = "hadoop dfs -put "+ filepath + " " + config.hadoopBasePath + JobName;
        let moveStatus = execSync(copyFilesToHadoop);
	//console.log("Script moved to HDFS");
    logger.info("Script moved to HDFS");
	//console.log("Removing Script file from local");
    logger.info("Removing Script file from local");
        let removeLocalFiles = "rm -f "+ filepath;
        let removeStatus = execSync(removeLocalFiles);
	//console.log("MoveScriptToHadoop Function call end");
    logger.info("MoveScriptToHadoop Function call end");
    }

	common.submitOozieJob = function(JobName){
		//console.log("SubmitOozieJob Function start");
         logger.info("SubmitOozieJob Function start");
		let oozieJobStmt = "nohup " + config.oozieHome + "bin/oozie job -oozie " + config.oozieWebHost + " -config "+config.localBasePath+JobName+"/job.properties -run &"
	console.log('command', oozieJobStmt);
		let runStatus = execSync(oozieJobStmt);	
		//console.log("SubmitOozieJob Function end");
        logger.info("SubmitOozieJob Function end");
	}

}(common));

module.exports = common;
