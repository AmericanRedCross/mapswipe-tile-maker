var flow = require("flow");
var settings = require("./settings/settings");
var mysql = require("mysql");
var CronJob = require('cron').CronJob;

var PostGresHelper = require("./routes/PostGresHelper.js");
var pghelper = new PostGresHelper();

var MySqlHelper = require("./routes/MySqlHelper.js");
var mysqlhelper = new MySqlHelper();

var ETL = require("./routes/ETL.js");
var etl = new ETL();

var job = new CronJob({
  cronTime: '00 00 00 * * *',
  onTick: function() {
    // Runs every day at 00:00:00
    etl.etl(function(){ console.log("sync complete")});
  },
  start: true,
  runOnInit: false
});

flow.exec(
  function(){
    // postgresql should be running and 'mapswipe' db already created
    // check to see if postgis is enabled
    pghelper.query("SELECT postgis_full_version();", this);
  }
  ,function(err, result){
    if(err){
      console.log("postgis not enabled. creating extensions.")
      pghelper.query("CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology;", this);
    } else {
      // console.log(result[0].postgis_full_version);
      this(); // move to next step
    }
  }
  ,function(err, result){
    if(err) console.log(err);
    pghelper.checkIfTableExists("projects", this);
  }
  ,function(err, exists){
    if(!exists){
      console.log("creating projects table");
      var sql = "CREATE TABLE projects " +
        "( project_id   numeric primary key, " +
          "objective    text, " +
          "name         text" +
        "); ";
      pghelper.query(sql, this);
    } else {
      console.log("projects table already exists");
      this(); // move to next step
    }
  }
  ,function(err, result){
    if(err) console.log(err);
    pghelper.checkIfTableExists("results", this);
  }
  ,function(err, exists){
    if(!exists){
      console.log("creating results table");
      var sql = "CREATE TABLE results " +
        "( id           text primary key," +
          "project_id   numeric, " +
          "one          numeric, " +
          "two          numeric, " +
          "three        numeric, " +
          "error        numeric, " +
          "timestamp    numeric " +
        "); "+
        "SELECT AddGeometryColumn('results','geom',4326,'POLYGON',2);";
      pghelper.query(sql, this);
    } else {
      console.log("results table already exists");
      this(); // move to next step
    }
  }
  ,function(err, result){
    console.log("start-up checks complete");
    setTimeout(function(){
      etl.run(function(){ console.log("startup sync complete") })
    }, 3000);

  }
);
