var swarmdb = require("swarmdb.js");

var connection = new swarmdb.createConnection({
    host: "localhost",
    port: 2001
});

var tableowner = "ADDRESS_IN_YOUR_CONFIG_FILE";

// create table
var columns = [
    { "indextype": 1, "columnname": "email", "columntype": 2, "primary": 1 },
    { "indextype": 2, "columnname": "age", "columntype": 1, "primary": 0 }
];
connection.createTable("test", columns, function (err, result) {
    if (err) throw err;
    console.log("create table response: " + result);
});

// put
connection.put("test", tableowner, [ {"age":1,"email":"test001@wolk.com"} ], function (err, result) {
    if (err) throw err;
    console.log("put response 1: " + result);
});

connection.put("test", tableowner, [ {"age":2,"email":"test002@wolk.com"} ], function (err, result) {
    if (err) throw err;
    console.log("put response 2: " + result);
});

connection.put("test", tableowner, [ {"age":3,"email":"test003@wolk.com"} ], function (err, result) {
    if (err) throw err;
    console.log("put response 3: " + result);
});


// get
connection.get("test", tableowner, "test001@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response 1: " + result);
});

connection.get("test", tableowner, "test003@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response 2: " + result);
});

// query
connection.query("select email, age from test where email = 'test001@wolk.com'", tableowner, function (err, result) {
    if (err) throw err;
    console.log("query response 1: " + result);
});