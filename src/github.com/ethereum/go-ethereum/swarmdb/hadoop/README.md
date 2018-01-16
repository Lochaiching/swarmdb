


# SwarmDB Dataflow from Farmers+Buyers to Validators

All SwarmDB node types (farmers/buyers, validators) are required to register on child chain ENS in order to participate in swarmDB services.

We will develop a sharded processing model where:
* Buyers are responsible for compensating Farmers for their network usages. 
* Farmers are responsible for storing chunks when accepting buyers' requests.
* Validators are responsible for tallying/validating chunks in the shard they have been randomly assigned to.
## Farmers to Validators

Farmer nodes will generate farmerlog periodically with the following format:

     $ cat farmerlog_<Address>_<Unix Timestamp>.txt
     {"farmer":"0x68988336d54ecd93bd9098607c32497bd7df0015","chunkID":"2b3b7615443069fa6886eec0283d23b09b54a906f78df2d1e537db0ebbf148ca","chunkBD":1515542149,"chunkSD":1515542150,"rep":5,"renewable":1}
     ...     
     {"farmer":"0x68988336d54ecd93bd9098607c32497bd7df0015","chunkID":"215b892e8f44980fbbbcc3ee8f92ddad558bcfdcc0f89d6d4852aa62e1a63ebb","chunkBD":1515542149,"chunkSD":1515542157,"rep":4,"renewable":0}
    
In future, farmerlogs are expected to live in Swarm (and should be retrievable by content hash on ENS registry). But for now, farmerlog will be _retrieved on demand_ by validators via:

     http://<ip:port>/<Farmer Address>

where each farmer node's `<ip:port>` can be looked up from Wolk Chain ENS Registry.
 
Validators receive this as input and run a mapreduce Hadoop job to summarize what has happened to farmers in their shard:

     $ cat  farmerlog_*.txt | php validator_farmer-map.php | sort | php validator_farmer-reduce.php
     {"validator":"validator_publickey","farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunks":4,"valid":4}
     {"validator":"validator_publickey","farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunks":2,"valid":2}

# Buyers to Validators

Buyers nodes will generate buyerlog per address with the following format:
    
     $ cat buyerlog_<Address>_<Unix Timestamp>.txt
	{"buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunkID":"2b3b7615443069fa6886eec0283d23b09b54a906f78df2d1e537db0ebbf148ca","chunkBD":1515542149,"chunkSD":1515542149,"rep":5,"renewable":1,"sig":"","smash":""}
     ...
    {"buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunkID":"215b892e8f44980fbbbcc3ee8f92ddad558bcfdcc0f89d6d4852aa62e1a63ebb","chunkBD":1515542149,"chunkSD":1515542149,"rep":5,"renewable":1,"sig":"","smash":""}

Similarly,  multiple buyerlogs from a single buyer node can be _retrieved on demand_ via:
  
     http://<ip:port>/<Buyer Address_0>
     ...
     http://<ip:port>/<Buyer Address_1>

Validators responsible for given shard will receive these as inputs and run a mapreduce Hadoop job to check for chunk health:

     $ cat buyerlog_*.txt | php validator_buyer-map.php | sort | php validator_buyer-reduce.php
     {"validator":"validator_publickey","buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunks":3,"valid":3}
     ...
     {"validator":"validator_publickey","buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunks":3,"valid":3}

# Aggregators

Aggregators take multiple inputs from validators and insurers:

    {"validator":"validator1_publickey","farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunks":4,"valid":4}
    {"validator":"validator1_publickey","farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunks":2,"valid":2}
    {"validator":"validator2_publickey","farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunks":4,"valid":4}
    {"validator":"validator2_publickey","farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunks":2,"valid":2}
    ...
    {"insurer":"insurer1_publickey","buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunks":3,"valid":3}
    {"insurer":"insurer1_publickey","buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunks":5,"valid":5}
    {"insurer":"insurer2_publickey","buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunks":3,"valid":3}
    {"insurer":"insurer2_publickey","buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunks":5,"valid":5}

and seek to achieve 2/3 consensus for their shard for each farmer and buyer

    $ cat aggregator-input-?.txt | php aggregator-reduce.php 
    {"buyerID":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","tally":1}
    {"buyerID":"0xd80a4004350027f618107fe3240937d54e46c21b","tally":1}
    {"farmerID":"0xd80a4004350027f618107fe3240937d54e46c21b","tally":1}
    {"farmerID":"0xd80a4004350027f618107fe3240937d54e46c21b","tally":1}

and finally submit a transaction (within the current gas limit) subject to the constraint:

    (*) the sum of all farmer inputs must be less than the buyer inputs

using the previous blocks `storagecost` and `bandwidthcost` with a scaling operation.

# Major Design Questions / Prototyping Path

* How are the local SWARMDB node inputs of `targetstoragecost` and `targetbandwidthcost` sent by the SWARMDB nodes up to validators and insurers through to aggregators?

* How is minReplication and maxReplication implemented?   (The stub functions do not do anything)

* How is the constraint (*) math done with in the Wolk child chain?
