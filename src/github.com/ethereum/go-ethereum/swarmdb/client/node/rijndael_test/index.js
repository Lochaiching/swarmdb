// reference: https://crypto.stackexchange.com/questions/48911/node-js-openssl-crypto-library-equivalent-of-a-rijndael-implementation
// AES is Rijndael, aes-128-ecb should be standard AES for 128 bit keys VS sgx_rijndael128GCM_encrypt 

var keythereum = require("keythereum");
var crypto = require("crypto");
var eccrypto = require("eccrypto");
var base64 = require('base64-js');
var rijndael = require('js-rijndael');

var dk = keythereum.create();
// generate private key
var privateKey = dk.privateKey;
// retrieve public key from private key
var publicKey = eccrypto.getPublic(privateKey);
console.log("private key: " + privateKey.toString("hex"));
console.log("public key: " + publicKey.toString("hex"));

var key = [].slice.call(base64.toByteArray(privateKey.toString('base64')));
// initialization vector
var iv = [].slice.call(base64.toByteArray(crypto.randomBytes(16).toString('base64')));
console.log("Initialization Vector: " + base64.fromByteArray(iv));
// encode messsage string in base64
var message = [].slice.call(base64.toByteArray(new Buffer("hello world").toString('base64')));

//encrypt and decrypt take byte arrays as inputs (regular arrays)
var encryptedByteArray = rijndael.encrypt(message, iv, key, "rijndael-128", "ecb");
console.log("Encrypted message: " + base64.fromByteArray(encryptedByteArray));

var clearText = String.fromCharCode.apply(this, rijndael.decrypt(encryptedByteArray, iv, key, "rijndael-128", "ecb"));
console.log("Decrypted message: " + clearText);