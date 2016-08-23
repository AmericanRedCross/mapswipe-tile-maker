var flow = require("flow");
var tilebelt = require('tilebelt');

var PostGresHelper = require("./PostGresHelper.js");
var pghelper = new PostGresHelper();

var MySqlHelper = require("./MySqlHelper.js");
var mysqlhelper = new MySqlHelper();


var ETL = function() {
  // find the time to start at for querying for sync
  var self = this;
  flow.exec(
    function(){
      var sql = "SELECT MAX(timestamp) FROM results;";
      pghelper.query(sql, this)
    }
    ,function(err, rows){
      if(rows[0]['max'] === null){
        // no results in pg database, find the earliest result in app database
        mysqlhelper.query("SELECT MIN(timestamp) AS min FROM results;", this);
      } else {
        // we've synced before, set checkpoint as most recent result in the pg database
        self.checkpoint = rows[0]['max'];
      }
    }
    ,function(err, rows){
      // we haven't synced before, set checkpoint as oldest result in app database
      self.checkpoint = rows[0]['min']
    }
  )
}

ETL.prototype.run = function(cb) {
     var self = this;
     self.now = new Date().getTime();
     // sync if most recent checkpoint is more than an hour ago
     if(self.now - self.checkpoint <= 3600000){
       console.log("nothing to do right now")
       cb();
     } else {
       console.log("we're doing it")
       self.sync(cb)
     }
}

ETL.prototype.sync = function(cb) {
  var self = this;
  flow.exec(
    function() {
      var sql = "SELECT * FROM results WHERE timestamp<" + self.now +
      " AND timestamp<" + (self.checkpoint + 86400000) + // 12 hours
      " AND timestamp>=" + self.checkpoint + ";";
      self.checkpoint = (self.checkpoint + 86400000 < self.now) ? self.checkpoint + 86400000 : self.now;

      mysqlhelper.query(sql, this);
    }
    ,function(err, rows) {
      if(rows.length > 0){
        console.log("inserting " + rows.length + " rows ")
        for(var i=0; i<rows.length; i++){
          self.insertResult(rows[i],this.MULTI());
        }
      } else {
        console.log("no results to insert");
        this();
      }
    }
    ,function(){
      self.run(cb);
    }
  )
}

ETL.prototype.insertResult = function(ob, cb){

  var location = ob['task_id'].split('-');
  // reorder because tilebelt expects [x,y,z]
  var tile = [parseFloat(location[1]), parseFloat(location[2]), parseFloat(location[0])];
  var geometry = tilebelt.tileToGeoJSON(tile);
  var sql = "INSERT INTO results (id, project_id, one, two, three, error, timestamp, geom) VALUES (" +
    "'" + ob.project_id + "-" + ob.task_id + "'," +
    "'" + ob.project_id + "',";
  switch(ob['result']){
      case 1:
        sql += "1,0,0,0,";
        break;
      case 2:
        sql += "0,1,0,0,";
        break;
      case 3:
        sql += "0,0,1,0,";
        break;
      default:
        sql += "0,0,0,1,";
    }
  sql += ob.timestamp + "," +
    "ST_GeomFromGeoJSON('{" +
    '"type":"Polygon","coordinates":' +
    JSON.stringify(geometry.coordinates) + "," +
    '"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}' + "')) " +
    "ON CONFLICT (id) DO UPDATE SET " +
    "timestamp = GREATEST(EXCLUDED.timestamp, results.timestamp), ";
  switch(ob['result']){
      case 1:
        sql += "one = results.one + 1 "
        break;
      case 2:
        sql += "two = results.two + 1 "
        break;
      case 3:
        sql += "three = results.three + 1 "
        break;
      default:
        sql += "error = results.error + 1 "
    }
  pghelper.query(sql, cb);

}


module.exports = ETL;
