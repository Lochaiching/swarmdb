var net = require('net');

var HOST = '127.0.0.1';
var PORT = 9500;

var client = new net.Socket();
client.connect(PORT, HOST, function() {
    console.log('connected to: ' + HOST + ':' + PORT);
    console.log('Sending hello world to Server');
    client.write('hello world');
});

// Add a 'data' event handler for the client socket
// data is what the server sent to this socket
client.on('data', function(data) {
    console.log("Receive from Server: " + data);
    // Close the client socket completely
    // client.destroy();
    
});

// Add a 'close' event handler for the client socket
client.on('close', function() {
    console.log('Connection closed');
});