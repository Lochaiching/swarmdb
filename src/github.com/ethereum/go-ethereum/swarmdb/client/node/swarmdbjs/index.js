var net = require('net');
var Web3 = require('web3');

function Connection(options) {
    var client = new net.Socket();
    this.web3 = new Web3(new Web3.providers.HttpProvider("https://mainnet.infura.io/pJJrBQxSLPzuFF8KGmi0")); 
    this.signChallenge = false; 
    this.buffer = [];
    this.waiting_for_response = false;

    var that = this;
    this.client = client.connect(options.port, options.host, function() {  
        console.log('CONNECTED TO: ' + options.host + ':' + options.port);
    });
    
    this.promise = new Promise((resolve, reject) => {
        that.client.on('data', function(data) {
            if (!that.signChallenge) {
                var sig = that.web3.eth.accounts.sign(data.toString().replace(/\n|\r/g, ""), '0x4b0d79af51456172dfcc064c1b4b8f45f363a80a434664366045165ba5217d53');
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
    }
};

exports.createConnection = function createConnection(config) {
    return new Connection(config);
};