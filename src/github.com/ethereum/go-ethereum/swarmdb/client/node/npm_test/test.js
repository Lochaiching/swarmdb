var swarmdb = require("swarmdb.js");

var connection = new swarmdb.createConnection({
    host: "10.128.0.29",
    //host: "127.0.0.1",
    port: 2001
});


// create table
var columns = [
    { "indextype": 2, "columnname": "email", "columntype": 2, "primary": 0 },
    { "indextype": 2, "columnname": "age", "columntype": 1, "primary": 1 }
];
connection.createTable("test", tableowner, columns, function (err, result) {
    if (err) throw err;
    console.log("create table response: " + result);
});

var tableowner = "9982ad7bfbe62567287dafec879d20687e4b76f5";
var tableowner = "9982ad7bfbe62567287dafec879d20687e4b76f5ZZ";
// // put
connection.put("testzz", tableowner, [{"Cells": {"age":1,"email":"test001@wolk.com"}}], function (err, result) {
    if (err) throw err;
    console.log("put response: " + result);
});

connection.put("test", tableowner, [{"Cells": {"age":2,"email":"test002@wolk.com"}}], function (err, result) {
    if (err) throw err;
    console.log("put response: " + result);
});

connection.put("test", tableowner, [{"Cells": {"age":3,"email":"test003@wolk.com"}}], function (err, result) {
    if (err) throw err;
    console.log("put response: " + result);
});

// get
connection.get("test", tableowner, "2", function (err, result) {
    if (err) throw err;
    console.log("get response: " + result);
});
/*
connection.get("test", tableowner, "test003@wolk.com", function (err, result) {
    if (err) throw err;
    console.log("get response: " + result);
});

// query

connection.query("select email, age from test where email = 'test001@wolk.com'", tableowner, function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});

connection.query("select email, age from test where email = 'test002@wolk.com'", tableowner, function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});
*/

connection.query("select email, age from test where age > 1", tableowner, function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});

connection.query("select email, age from test where age = 3", tableowner, function (err, result) {
    if (err) throw err;
    console.log("query response: " + result);
});

var columns = [
    { "indextype": 1, "columnname": "email", "columntype": 2, "primary": 1 },
    { "indextype": 1, "columnname": "name", "columntype": 2, "primary": 0 },
    { "indextype": 2, "columnname": "age", "columntype": 1, "primary": 0 }
];

connection.createTable("contacts", columns, function (err, result) {
    if (err) {
        throw err;
    }
    console.log(result);
});

var tableowner = "1234567890123456789012345678901234567890";

connection.query("INSERT INTO contacts (email, name, age) VALUES ('paul@gmail.com', 'Paul', 12)", tableowner, function (err, result) {
        if (err) {
            throw err;
        }
        console.log("insert response:" + result);
});
