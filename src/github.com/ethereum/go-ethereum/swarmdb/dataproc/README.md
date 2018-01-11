

# SWARMDB Dataflow from Farmers+Buyers to Aggregators to Validators

All node types (farmers/buyers SWARMDB, validators, aggregators, miners) of the child chain register their existence (their public key [Eth address], ip address/ports) in a public SWARMDB table.

We will be developing a sharded processing model:
* Farmers/Buyers are responsible for chunks on their node and reveal farmerlog/buyerlog outputs
* Aggregators are responsible for tallying for each chunk which farmers are correctly storing chunks and buyers that are insuring chunks
* Validators are responsible for taking the aggregator view and tallying the potential costs and potential payouts 

## Farmer Logs + Buyer Logs to Aggregators

Farmers SWARMDB nodes generate outputs representing what claims are made:

     $ cat farmerlog-input.txt
     {"farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500}
     {"farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunkID":"4b09668b93c718092a408c4222867968fcd3ad98","ip":"127.0.0.1","port": 8500}
     {"farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunkID":"d368b1c09e7ddfb6aff24e8e6f181ffeea905d31","ip":"127.0.0.1","port": 8500}
     {"farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500}
     {"farmer":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500}
     {"farmer":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500}

and also generate outputs representing the insured chunks

     $ cat buyerlog-input.txt
     {"buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500,"minReplication":3,"maxReplication":5}
     {"buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunkID":"4b09668b93c718092a408c4222867968fcd3ad98","ip":"127.0.0.1","port": 8500,"minReplication":3,"maxReplication":5}
     {"buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500,"minReplication":2,"maxReplication":15}
     {"buyer":"0xd80a4004350027f618107fe3240937d54e46c21b","chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","ip":"127.0.0.1","port": 8500,"minReplication":3,"maxReplication":5}
     {"buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","ip":"127.0.0.1","port": 8500,"minReplication":2,"maxReplication":15}
     {"buyer":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","chunkID":"d368b1c09e7ddfb6aff24e8e6f181ffeea905d31","ip":"127.0.0.1","port": 8500,"minReplication":3,"maxReplication":5}

These can be stored in SWARM but for now are being retrieved on demand by the aggregators from the http interface of SWARMDB:

     http://ip:port/farmer
     http://ip:port/buyerlog

Validators receive this as input and run a mapreduce Hadoop job to summarize what has happened to farmers in their shard:

           $ cat buyerlog-input.txt farmerlog-input.txt | php aggregator-map.php | sort | php aggregator-reduce.php
           {"chunkID":"1d4b6e4aa86d48c464c9adf83940d4e00df8affc","buyers":["0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","0xd80a4004350027f618107fe3240937d54e46c21b"],"minReplication":1,"maxReplication":15,"farmers":["0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","0xfd990c3c42446f6705dd66376bf5820cf2c09527"]}
           {"chunkID":"4b09668b93c718092a408c4222867968fcd3ad98","buyers":["0xd80a4004350027f618107fe3240937d54e46c21b"],"minReplication":1,"maxReplication":5,"farmers":["0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"]}
           {"chunkID":"aeec6f5aca72f3a005af1b3420ab8c8c7009bac8","buyers":["0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","0xd80a4004350027f618107fe3240937d54e46c21b"],"minReplication":1,"maxReplication":15,"farmers":["0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","0xfd990c3c42446f6705dd66376bf5820cf2c09527"]}
           {"chunkID":"d368b1c09e7ddfb6aff24e8e6f181ffeea905d31","buyers":["0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8"],"minReplication":1,"maxReplication":5,"farmers":["0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"]}

# Validators

Validators take multiple inputs from aggregators and put it together :

           $ cat validator-input.txt | php validator-map.php | sort | php validator-reduce.php 
           {"validator":"validator_publickey","id":"0xcb2fa2c491451cac943bb5a0261eb101cc36a4f8","b":3,"f":0}
           {"validator":"validator_publickey","id":"0xd80a4004350027f618107fe3240937d54e46c21b","b":1,"f":0}
           {"validator":"validator_publickey","id":"0xf6b55acbbc49f4524aa48d19281a9a77c54de10f","b":0,"f":4}
           {"validator":"validator_publickey","id":"0xfd990c3c42446f6705dd66376bf5820cf2c09527","b":0,"f":2}

and seek to achieve 2/3 consensus for their shard for each farmer and buyer, and finally submit a transaction (within the current gas limit) subject to the constraint:

    (*) the sum of all farmer inputs must be less than the buyer inputs

using the previous blocks `storagecost` and `bandwidthcost` with a scaling operation.

# Major Design Questions / Prototyping Path

* How are the local SWARMDB node inputs of `targetstoragecost` and `targetbandwidthcost` sent by the SWARMDB nodes up to validators and insurers through to aggregators?

* How is the constraint (*) math done with in the Wolk child chain?






