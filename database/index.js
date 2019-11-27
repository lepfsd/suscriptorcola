var mysql = require('mysql');

console.log('mysql-pre');
var con = mysql.createConnection({
	host: "51.15.95.84",
	user: "dataset",
	password: "Aquinoentras22@",
	database: "dmp"
});

con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});

module.exports = con;