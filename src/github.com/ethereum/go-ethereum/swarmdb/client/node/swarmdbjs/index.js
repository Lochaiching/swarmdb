var net = require('net');
var fs = require('fs');
var Web3 = require('web3');
var keythereum = require("keythereum");

const KEYSTORE_PATH = "/swarmdb/data/keystore/UTC--2017-12-18T23-58-06.344Z--9982ad7bfbe62567287dafec879d20687e4b76f5";
const KEYSTORE_PASSWORD = "wolkwolkwolk";

function Connection(options) {
    var client = new net.Socket();
    this.web3 = new Web3(new Web3.providers.HttpProvider("https://mainnet.infura.io/pJJrBQxSLPzuFF8KGmi0")); 
    var privateKey;
    
    this.signChallenge = false; 
    this.buffer = [];
    this.waiting_for_response = false;

    var that = this;
    this.client = client.connect(options.port, options.host, function() {  
        console.log('CONNECTED TO: ' + options.host + ':' + options.port);
        fs.readFile(KEYSTORE_PATH, 'utf-8', function (err, keystore) {
            if (err) console.log(err);
            var privateKeyBuffer = keythereum.recover(KEYSTORE_PASSWORD, JSON.parse(keystore));
            privateKey = privateKeyBuffer.toString("hex");
            console.log("private key read from keystore: " + "0x" + privateKey);
        });
    });

    this.client.on('close', function() {
        console.log('Server side connection closed');
    });

    this.client.on('error', function(err) {
        console.log("Error: " + err);
    });
    
    this.promise = new Promise((resolve, reject) => {
        that.client.on('data', function(data) {
            if (!that.signChallenge) {
                var sig = that.web3.eth.accounts.sign(data.toString().replace(/\n|\r/g, ""), "0x" + privateKey);
                console.log("Sending signature: " + sig.signature.slice(2));
                that.signChallenge = true;
                that.verify = that.client.write(sig.signature.slice(2) + "\n", null, function() {
                    resolve();
                });   
            }
            that.waiting_for_response = false;
            if (that.buffer.length) {
                var pair = that.buffer.shift();
                var handler = pair[0];
                process.nextTick(function() {
                    if (data.err) {
                        handler("err", null);
                    }
                    else {
                        handler(null, data.toString().trim());
                    }    
                });
                that.flush();
            } 
        });
    });
};
Connection.prototype = {
    request: function(msg, handler) {
        this.buffer.push([handler, msg]);
        this.flush();
    },
    flush: function() {
        var pair = this.buffer[0];
        if (pair && !this.waiting_for_response) {
            this.client.write(pair[1]);
            this.waiting_for_response = true;
        }
    },
    createTable: function(table, columns, callback) {
        var that = this;
        var msg = JSON.stringify({
            "requesttype": "CreateTable",
            "table": table,
            "columns": columns
        }) + "\n";
        this.promise.then(() => {
            that.request(msg, callback);
        });
    },
    get: function(table, key, callback) {
        var that = this;
        var msg = JSON.stringify({
            "requesttype": "Get",
            "table": table,
            "key": key,
            "columns": null
        }) + "\n";
        this.promise.then(() => {
            that.request(msg, callback);
        });
    },
    put: function(table, row, callback) {
        var that = this;
        var msg = JSON.stringify({
            "requesttype": "Put",
            "table": table,
            "row": row,
            "columns": null
        }) + "\n";
        this.promise.then(() => {
            that.request(msg, callback);
        });
    },
    query: function(queryStatement, callback) {
        var that = this;
        var msg = JSON.stringify({
            "requesttype": "Query",
            "RawQuery": queryStatement
        }) + "\n";
        this.promise.then(() => {
            that.request(msg, callback);
        });
    }
};

exports.createConnection = function createConnection(config) {
    return new Connection(config);
};