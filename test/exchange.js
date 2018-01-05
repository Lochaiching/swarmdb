// compute exchange rate stats 
var Web3 = require('web3');
var provider = "https://mainnet.infura.io/metamask";
// provider = "https://mainnet.infura.io/KBTX5agPUfCByr3G0Kvx";
var web3 = new Web3(); 
web3.setProvider(new Web3.providers.HttpProvider(provider));
var bancorabi = [{"constant":true,"inputs":[{"name":"_supply","type":"uint256"},{"name":"_reserveBalance","type":"uint256"},{"name":"_reserveRatio","type":"uint8"},{"name":"_depositAmount","type":"uint256"}],"name":"calculatePurchaseReturn","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_baseN","type":"uint256"},{"name":"_baseD","type":"uint256"},{"name":"_expN","type":"uint256"},{"name":"_expD","type":"uint256"},{"name":"_precision","type":"uint8"}],"name":"power","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}];
var wolkincabi=[{"constant":true,"inputs":[],"name":"percentageETHReserve","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"contributorTokens","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"reserveBalance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}];

function calculatePurchaseReturn(bancor, wolkinc) 
{
    var e = { totalSupply: 0, contributorTokens: 0, percentageETHReserve: 0, reserveBalance:  0, purchaseReturn : 0 };
    var allPromise = Promise.all([ wolkinc.methods.totalSupply().call().then(function(result) { e.totalSupply = result; }),
                                   wolkinc.methods.contributorTokens().call().then(function(result) { e.contributorTokens = result; }),
                                   wolkinc.methods.percentageETHReserve().call().then(function(result) { e.percentageETHReserve = result; }),
                                   wolkinc.methods.reserveBalance().call().then(function(result) { e.reserveBalance = result; })
                                 ]);
    allPromise.then(function(result) {
        // console.log( "contributorTokens: " + e.contributorTokens + " percentageETHReserve: " + e.percentageETHReserve + " reserveBalance: " + e.reserveBalance)
        bancor.methods.calculatePurchaseReturn(web3.utils.toBN(e.contributorTokens),web3.utils.toBN(e.reserveBalance), e.percentageETHReserve,web3.utils.toBN("1000000000000000000")).call()
            .then(function(result2) { e.purchaseReturn = result2; console.log(JSON.stringify(e)); } )});
}


var bancor = new web3.eth.Contract(bancorabi, '0xa8515b27487b327d1ffa257f89b657fe87672980');
var wolkinc = new web3.eth.Contract(wolkincabi, '0xf6b55acbbc49f4524aa48d19281a9a77c54de10f');
calculatePurchaseReturn(bancor, wolkinc);
