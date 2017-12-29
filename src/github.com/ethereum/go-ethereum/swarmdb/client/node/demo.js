var swarmdb = require("./swarmdbjs");

var connection = new swarmdb.createConnection({
    host: "127.0.0.1",
    port: 2000
});

// create table
var columns = [
    { "indextype": 2, "columnname": "email", "columntype": 2, "primary": 1 },
    { "indextype": 1, "columnname": "age", "columntype": 1, "primary": 0 }
];
connection.createTable("test", columns, function (err, result) {
    if (err) throw err;
    console.log("create table response: " + result);
});

// // put
connection.put("test", {"age":"1","email":"test001@wolk.com"}, function (err, result) {
    if (err) throw err;
    console.log("put response: " + result);
});

connection.put("test", {"age":"2","email":"test002@wolk.com"}, function (err, result) {
    if (err) throw err;
    console.log("put response: " + result);
});

// get
connection.get("test", "test001@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response: " + result);
});

connection.get("test", "test002@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response: " + result);
});

// query
connection.query("select email, age from test where email = 'test001@wolk.com'", function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});

connection.query("select email, age from test where email = 'test002@wolk.com'", function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});