let fs = require("fs");
let config = require('../config');
let path = require('path');

module.exports.createTwitterProperties = function(obj){

let twitter_properties ="";
let nl = "\n"
let debug = "debug=true" + nl;
let p_consKey = "oauth.consumerKey=" + obj.consumer_key + nl;
let p_consSecret = "oauth.consumerSecret=" + obj.secret_key + nl;
let p_accToken =  "oauth.accessToken=" + obj.access_token + nl;
let p_secaccToken = "oauth.accessTokenSecret=" + obj.secret_access_token + nl;
let jsonenabled = "jsonStoreEnabled=true";

twitter_properties = debug + p_consKey + p_consSecret + p_accToken + p_secaccToken + jsonenabled;

console.log(twitter_properties);
//let datetime = new Date().getHours() + "_" + new Date().getMinutes() + "_" + new Date().getSeconds();

//console.log("File  : " + __dirname + config.twitter_properties_filepath + "_backup_" +  datetime );

let dirn = path.resolve(__dirname, '..');
console.log("Folder : " + dirn);

fs.writeFileSync(dirn + config.twitter_properties_filepath , twitter_properties, {encoding:'utf8',flag:'w'});


}

module.exports.createConfProperties = function(obj){

let conf_properties ="";
let nl = "\n"
let metabroker = "metadata.broker.list=172.31.17.159:9092" + nl;
let p_consKey =  "bootstrap.servers=172.31.17.159:9092" + nl;
let p_consSecret = "acks=all" + nl;
let p_accToken =  "retries=0" + nl;
let p_secaccToken = "batch.size=16384" + nl;
let p_linger = "linger.ms=1"+ nl;
let p_buffer = "buffer.memory=33554432";
let p_keyser = "key.serializer=org.apache.kafka.common.serialization.StringSerializer" + nl;
let p_valser = "value.serializer=org.apache.kafka.common.serialization.StringSerializer" + nl;
let p_tname1 = "topicname1=twitter-data" + nl;
let p_tname2 = "topicname2=twitter-rawdata" + nl;
let p_twitterfilter = "twitterfilter=" +  obj.keyword ;

conf_properties = metabroker + p_consKey + p_consSecret + p_accToken + p_secaccToken + p_linger + p_buffer + 
				 p_keyser +  p_valser + p_tname1 + p_tname2 + p_twitterfilter;

console.log(conf_properties);

let dirn = path.resolve(__dirname, '..');
console.log("Folder : " + dirn);


fs.writeFileSync(dirn + config.conf_properties_filepath , conf_properties, {encoding:'utf8',flag:'w'});

}