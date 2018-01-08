var express = require("express");
var bodyParser = require("body-parser");
var morgan = require("morgan");
var config = require("./config");
var cors = require('cors');
var router = require("./router/main");
var app = express();

app.use(cors());

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use(morgan('dev'));
app.use(express.static('public'));

global.__basedir = __dirname;

//****** Engine Logic Here ************* //
app.set('view engine','ejs');
app.engine('html', require('ejs').renderFile); //Specify view engine

var mysqlapi = require('./app/routes/connect')(app, express);
//var mssqlapi = require('./app/routes/mssqlconnect')(app, express);
app.use('/bigdataframework/api', mysqlapi);
//app.use('/bigdataframework/mssqlapi', mssqlapi);
router(app);

app.listen(config.port, function (err) {
    if (err) {
        console.log(err);
    } else {
        console.log("Server up and running on port ; " + config.port);
    }
});
