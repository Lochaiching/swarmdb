var keythereum = require("keythereum");
var crypto = require("crypto");
var eccrypto = require("eccrypto");

var dk = keythereum.create();
// generate private key
var privateKey = dk.privateKey;
// retrieve public key from private key
var publicKey = eccrypto.getPublic(privateKey);
console.log("private key: " + privateKey.toString("hex"));
console.log("public key: " + publicKey.toString("hex"));

// alternative: generate pk, sk by node built-in crypto
// var privateKey = crypto.randomBytes(32);
// var publicKey = eccrypto.getPublic(privateKey);
// console.log("private key: " + privateKey.toString("hex"));
// console.log("public key: " + publicKey.toString("hex"));

var msgStr = "challenge";
var msgHash = crypto.createHash("sha256").update(msgStr).digest();
console.log("Hashed message: " + msgHash);

eccrypto.sign(privateKey, msgHash).then(function(sig) {
    console.log("Signature: " + sig);
    eccrypto.verify(publicKey, msgHash, sig).then(function() {
        console.log("Signature is GOOD");
    }).catch(function() {
        console.log("Signature is BAD");
    });
});