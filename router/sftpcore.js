let Client = require('ssh2-sftp-client');
let sftp = new Client();
let fs = require("fs");
let filename = "";
let hdfs_path = ""

let exec = require('child_process').exec;


module.exports.sftpCore = function(obj)
{
console.log("Within SFTPCOre ");

let host = obj.host;  //"172.16.243.116";
let port = obj.port; //"22";
let username = obj.username; //"mapr";
let password = obj.password; //"password";
let logpath = obj.path; //"/home/mapr/logs/error.log";
let filenamearray  = obj.path.split("/");
let hdfs_path = obj.hdfspath;
filename = filenamearray[filenamearray.length - 1];
let local_file_path = __dirname + "/" + filename;
let hadoop_command = "hadoop fs -put " + local_file_path + " " + hdfs_path;

sftp.connect({
    host: host,
    port: port,
    username: username,
    password: password
}).then(() => {
    console.log("Reading the File from Remote Server");
    return sftp.get(logpath);
}).then((data) => {
    console.log(data, 'the data info');
	console.log("Readstream : " + JSON.stringify(data._readableState.buffer.head.data));
	
	let txtdata = data._readableState.buffer.head.data;
	
	fs.writeFileSync(__dirname + "/" + filename,txtdata,'utf8');
	
	return 0;
}).then((response) => {

	if(response == 0)
	{
	  console.log("File Written. Ready to upload on HDFS");
	  
	  exec(hadoop_command, function (error, stdout, stderr) {							                              
	   if (error !== null) {
		 console.log('exec error: ' + error);
	   }
	  else
	  {
		  console.log("File successfully uploaded ");
		 // process.exit(0);
		 return 0;
	  }
}); 
	}
})
.catch((err) => {
    console.log(err, 'catch error');
});


}