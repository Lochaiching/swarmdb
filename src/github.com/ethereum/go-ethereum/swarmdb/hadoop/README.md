

# SWARMDB Dataflow from Farmers+Buyers to Validators+Insurers and Aggregators

All node types (farmers/buyers SWARMDB, validators, insurers, aggregators, miners) of the child chain register their existence (their public key [Eth address], ip address/ports).

We will be developing a sharded processing model:
* Farmers are responsible for chunks in their shard.
* Validators are responsible for farmers in their shard.
* Insurers are responsible for buyers in their shard.
* Aggregators are responsible for Validators and Insurers in their

## Farmers to Validators

Farmers SWARMDB nodes generate outputs like:

     $ cat validator-input.txt
     {"farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500}
     ...
     {"farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500}

which can be stored in SWARM but for now are being retrieved on demand by the validators.

     http://ip:port/farmer

Validators receive this as input and run a mapreduce Hadoop job to summarize what has happened to farmers in their shard:

     $ cat validator-input.txt | php validator-map.php | sort | php validator-reduce.php
     {"validator":"validator_publickey","farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunks":4,"valid":4}
     {"validator":"validator_publickey","farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunks":2,"valid":2}

# Buyers to Insurers

Buyers SWARMDB nodes generate outputs like:

     {"buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500,"minReplication":3,"maxReplication":5}
     ...
     {"buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500,"minReplication":2,"maxReplication":15}

which can be stored in SWARM but for now are being retrieved on demand by the insurers via the HTTP interface at 
  
     http://ip:port/buyer

Insurers receive this as input and run a mapreduce Hadoop job to summarize what has happened to buyers in their shard:

     $ cat insurer-input.txt | php insurer-map.php | sort | php insurer-reduce.php
     {"insurer":"insurer_publickey","buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunks":3,"valid":3}
     {"insurer":"insurer_publickey","buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunks":3,"valid":3}

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






