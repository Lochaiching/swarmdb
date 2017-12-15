var net = require('net');

var server = net.createServer();  
server.on('connection', handleConnection);

server.listen(9500, function() {  
  console.log('server listening at port 9500');
});

function handleConnection(conn) {  
  var remoteAddress = conn.remoteAddress + ':' + conn.remotePort;
  console.log('new client connection from ' + remoteAddress);

  conn.on('data', onConnData);
  conn.once('close', onConnClose);
  conn.on('error', onConnError);

  function onConnData(d) {
    console.log('connection data from ' + remoteAddress + ': ' + d);
    conn.write("server response: " + d);
    
  }

  function onConnClose() {
    console.log('connection from ' + remoteAddress + ' closed');
  }

  function onConnError(err) {
    console.log('Connection error on ' +  remoteAddress);
  }
}