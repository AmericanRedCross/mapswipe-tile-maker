var pg = require("pg");
var settings = require("../settings/settings.js");
var flow = require("flow");

var PostGresHelper = function() {
  // PostGIS Connection String
  this.conString = "postgres://" +
  settings.pg.user + ":" +
  settings.pg.password + "@" +
  settings.pg.server + ":" +
  settings.pg.port + "/" +
  settings.pg.database;

}

PostGresHelper.prototype.checkIfTableExists = function(tableName, cb) {

  //see if a table exists - ASSUMES public schema
  var sql = "SELECT EXISTS (SELECT 1 " +
  "FROM   pg_catalog.pg_class c " +
  "JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
  "WHERE  n.nspname = 'public' " +
  "AND    c.relname = '" + tableName + "' " +
  "AND    c.relkind = 'r'"+
  ");";

  this.query(sql, function (err, result) {
    cb(err, result[0].exists);
  });
  // result example..  [ { exists: false } ]
}

PostGresHelper.prototype.query = function(queryStr, cb) {
  pg.connect(this.conString, function(err, client, done) {
    if (err) {
      console.error('error fetching client from pool', err);
    }

    client.query(queryStr, function (queryerr, result) {
      //call `done()` to release the client back to the pool
      done();

      if (queryerr) {
        console.error('ERROR RUNNING QUERY:', queryStr, queryerr);
      }

      //common.log("Ran Query: " + queryStr)

      cb((err || queryerr), (result && result.rows ? result.rows : result));

    });
  });
};

module.exports = PostGresHelper;
