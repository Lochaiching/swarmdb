var net = require('net');
var Web3 = require('web3');

function Connection(options) {
    this.web3 = new Web3(new Web3.providers.HttpProvider("https://mainnet.infura.io/pJJrBQxSLPzuFF8KGmi0")); 
    this.signChallenge = false; 
    var client = new net.Socket();
    this.client = client.connect(options.port, options.host, function() {  
        console.log('CONNECTED TO: ' + options.host + ':' + options.port);
    });

    var ths = this;

    this.client.on('data', function(data) {
        console.log("from swarm server: " + data);
        if (!ths.signChallenge) {
            var sig = ths.web3.eth.accounts.sign(data.toString(), '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef');
            console.log(sig);
            // ths.client.write(sig.signature.slice(2));
            ths.signChallenge = true;
        }
    })

    this.client.on('error', function(err){
        console.log("Error: " + err.message);
    })
}


Connection.prototype.openTable = function openTable(table) {
    this.client.write(JSON.stringify({
        "requesttype": "OpenTable",
        "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
        "table": table,
        "columns": null
    }) + "\n");
};

Connection.prototype.get = function get(table, key, callback) {  
    this.client.write(JSON.stringify({
        "requesttype": "Get",
        "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
        "table": table,
        "key": key,
        "columns": null
    }) + "\n");
};

Connection.prototype.newGet = function newGet(table, key, callback) {  
    this.client.write(JSON.stringify({
        "requesttype": "OpenTable",
        "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
        "table": table,
        "columns": null
    }) + "\n");

    var that = this;
    
    this.client.on('data', function(data) {
        if (data.toString() == 'okay') {
            console.log('sending the Get JSON');
            that.client.write(JSON.stringify({
                "requesttype": "Get",
                "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
                "table": "contacts",
                "key": "rodneytest1@wolk.com",
                "columns": null
            }) + "\n");
        }
    });
};

Connection.prototype.put = function put(table, key, value, callback) {  
    this.client.write(JSON.stringify({
        "requesttype": "Put",
        "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
        "table": table,
        "key": key,
        "value": value,
        "columns": null
    }));
};

Connection.prototype.createTable = function createTable() {
    this.client.write(JSON.stringify({
        "requesttype": "CreateTable",
        "owner": "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f",
        "table": table,
        "columns": columns
    }));
};

Connection.prototype.query = function query(sql, callback) {  
};

exports.createConnection = function createConnection(config) {
    return new Connection(config);
};