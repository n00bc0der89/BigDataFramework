var mysql = require('mysql');
const config = require('../../config.js');
var pool;
module.exports = {
    getPool: function () {
        if (pool) return pool;
        pool = mysql.createPool({
            host: config.mysqlHost,
            port: config.mysqlPort,
            user: config.mysqlUser,
            password: config.mysqlPassword,
            database: 'stock_manager'
        });
        return pool;
    }
};

