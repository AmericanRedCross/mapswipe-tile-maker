var mysql = require("mysql");
var settings = require("../settings/settings");
var flow = require("flow");


var MySqlHelper = function() {
  this.pool = mysql.createPool({
    connectionLimit : 10,
    host: settings.mapswipe.host,
    user: settings.mapswipe.user,
    password: settings.mapswipe.password,
    database: settings.mapswipe.database
  });
}

MySqlHelper.prototype.query = function(queryStr, cb) {

  this.pool.getConnection(function(err, connection) {
    // Use the connection
    connection.query( queryStr, function(err, rows) {
      if(err){
        console.log("ERROR: " + err)
        // throw err;
      }
      console.log("QUERY: " + queryStr)
      cb(err, rows);
      // And done with the connection.
      connection.release();
      // Don't use the connection here, it has been returned to the pool.
    });
  });
};

module.exports = MySqlHelper;
