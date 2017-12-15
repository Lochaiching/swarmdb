var rs = require('jsrsasign');
var keythereum = require("keythereum");
var crypto = require("crypto");
var eccrypto = require("eccrypto");
var fs = require('fs');

const WALLET_PASSWORD = "wolk";

/*
// option 1: generate random private key and public key (Buffer)
var dk = keythereum.create();
// generate private key
var privateKey = dk.privateKey;
// retrieve public key from private key
var publicKey = eccrypto.getPublic(privateKey);
console.log("private key: " + privateKey.toString("hex"));
console.log("public key: " + publicKey.toString("hex"));
*/

/*
// option 2: generate private key and public key by node built-in crypto module (Buffer)
// var privateKey = crypto.randomBytes(32);
// var publicKey = eccrypto.getPublic(privateKey);
// console.log("private key: " + privateKey.toString("hex"));
// console.log("public key: " + publicKey.toString("hex"));
*/

// option 3: read private key from keystore (Buffer)
var privateKey;
var publicKey;
fs.readFile('keystore', 'utf-8', function (err, data) {
    if (err) {
        console.log(err);
    } else {
        // privateKey and publicKey are both Buffers
        privateKey = keythereum.recover(WALLET_PASSWORD, JSON.parse(data));
        publicKey = eccrypto.getPublic(privateKey);
        console.log("private key: " + privateKey.toString("hex"));
        console.log("public key: " + publicKey.toString("hex"));

        var msgStr = "sAFcbjKkwBOCtyNJFroPxWqn";
        console.log("Original message: " + msgStr);
        // sha256 hashed message is a Buffer
        var msgHash = crypto.createHash("sha256").update(msgStr).digest();
        console.log("sha256 hashed message: " + msgHash.toString('hex'));

        /* Documentation: https://kjur.github.io/jsrsasign/api/symbols/KJUR.crypto.ECDSA.html
           All the methods require input or output hex string instead of Buffer 
        */
        // define EC curve name
        var ecdsa = new rs.ECDSA({'curve': 'secp256k1'});
        // set private key and public key
        ecdsa.setPrivateKeyHex(privateKey.toString("hex"));
        ecdsa.setPublicKeyHex(publicKey.toString("hex"));
        // sign
        var signature = ecdsa.signHex(msgHash.toString('hex'), privateKey.toString("hex"));
        console.log("Signature: " + signature);
        //verify
        var result = ecdsa.verifyHex(msgHash.toString('hex'), signature, publicKey.toString("hex"));
        console.log("Script verify result: " + result);
        // verify input from https://kjur.github.io/jsrsasign/sample/sample-ecdsa.html
        var site_result = ecdsa.verifyHex("9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0", 
                                      "304402201a5ac18b616c0e54bb0af74b66d58a06a8d66ecefe308329fe411ca1a6b6fc1902203641b95599cfaac55237e5d8e9b811bc44d87db447a4bbad5d66ddd61ede49aa", 
                                      "0425cdda3d8cc57eb19d8798be07d43c078b68e0129e7946a93f84ef61103caeea952d34ce6b96f4821b368e6f4595f8c774060eddceecbd34cca842b14d90e411");
        console.log("Website verify result: " + site_result);
    }
});