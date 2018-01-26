var swarmdbAPI = require("swarmdb.js");

var swarmdb = new swarmdbAPI.createConnection({
    host: "localhost",
    port: 2001
});

var tableowner = "ADDRESS_IN_YOUR_CONFIG_FILE";

// create table
var columns = [
    { "indextype": 2, "columnname": "email", "columntype": 2, "primary": 1 },
    { "indextype": 2, "columnname": "age", "columntype": 1, "primary": 0 }
];
console.time('createTable took');
swarmdb.createTable("test", columns, function (err, result) {
    if (err) throw err;
    console.log("create table response: " + result);
    console.timeEnd('createTable took');
    console.log("\n");
});


// put
console.time('put 1 took');
swarmdb.put("test", tableowner, [ {"age":1,"email":"test001@wolk.com"}, {"age":2,"email":"test002@wolk.com"} ], function (err, result) {
    if (err) throw err;
    console.log("put response 1: " + result);
    console.timeEnd('put 1 took');
    console.log("\n");
});

console.time('put 2 took');
swarmdb.put("test", tableowner, [ {"age":3,"email":"test003@wolk.com"} ], function (err, result) {
    if (err) throw err;
    console.log("put response 2: " + result);
    console.timeEnd('put 2 took');
    console.log("\n");
});

// insert
console.time('insert query took');
swarmdb.query("insert into test (email, age) values ('test004@wolk.com', 4)", tableowner, function (err, result) {
    if (err) throw err;
    console.log("insert query response: " + result);
    console.timeEnd('insert query took');
    console.log("\n");
});

// get
console.time('get 1 took');
swarmdb.get("test", tableowner, "test001@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response 1: " + result);
    console.timeEnd('get 1 took');
    console.log("\n");
});

console.time('get 2 took');
swarmdb.get("test", tableowner, "test003@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response 2: " + result);
    console.timeEnd('get 2 took');
    console.log("\n");
});

// select
console.time('select query 1 took');
swarmdb.query("select email, age from test where email = 'test002@wolk.com'", tableowner, function (err, result) {
    if (err) throw err;
    console.log("select query response 1: " + result);
    console.timeEnd('select query 1 took');
    console.log("\n");
});

console.time('select query 2 took');
swarmdb.query("select email, age from test where age >= 2", tableowner, function (err, result) {
    if (err) throw err;
    console.log("select query response 2: " + result);
    console.timeEnd('select query 2 took');
});