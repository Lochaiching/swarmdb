// Node.js SWARMDB Basic Stub Test cases
var swarmdb = require('swarmdb');

// connection - opens a TCP/IP connection
var con = swarmdb.createConnection({
  host: "localhost:8500",
  keystore: "/tmp/wolk-wallet.json"
  password: "yourpassword"
});

// connect and get
con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
  con.put("contacts", { "email": "bruce@wolk.com", "gender": "M", "age": 27, "weight": 120.5 }, function (err, result) {
    if (err) throw err;
    console.log("1 record PUT");
  });
});

// get
con.get("contacts", "bruce@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("1 record GET");
});

// insert single row
var sql = "INSERT INTO contacts (email, gender, age, weight) VALUES ('sally@wolk.com', 'F', 38, 115.2)"
con.query(sql, function (err, result) {
    if (err) throw err;
    console.log("1 record SQL inserted");
});

// single row response
con.query("SELECT * FROM contacts where email = 'sally@wolk.com'", function (err, result, fields) {
    if (err) throw err;
    console.log(result);
});

// recordset processing
con.query("SELECT * FROM contacts where age >= 40", function (err, result, fields) {
    if (err) throw err;
    console.log(result);
});


var sql = "DELETE FROM contacts WHERE email = 'sally@wolk.com'";
con.query(sql, function (err, result) {
    if (err) throw err;
    console.log("Number of records deleted: " + result.affectedRows);
});


