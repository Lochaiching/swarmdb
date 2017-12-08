
Go Client (client.go, SGX1) ==> Go TCP/IP Server (server.go, SGX2)
  
  
On an OpenConnection("servername:port") call, instead of the client
passing in a username/password, the server will send a randomly
generated challenge/nonce string immediately upon connection.  The
client will be required to sign the challenge message in enclave SGX1
using the (pk, sk) and return back with (sig, message).  The server
will verify the signature of the message as being consistent with pk.
If consistent, the connection is then deemed secure, otherwise the
connection closes.

On a Put request from the client, the server will call StoreChunk, 
which must execute a function EncryptData in enclave SGX2.

On a Get request from the client, the server will call RetrieveChunk,
which must execute a function DecryptData in enclave SGX2.

To mediate operations with SGX2, we will modify KeyManager 
 https://github.com/wolktoken/swarm.wolk.com/blob/master/src/github.com/ethereum/go-ethereum/swarmdb/keymanager/keymanager.go




 
