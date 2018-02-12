


# How to Install SwarmDB

# Install Docker CE (Community Edition)
https://www.docker.com/community-edition#/download

### CentOS:
  - Installation instructions: https://docs.docker.com/engine/installation/linux/docker-ce/centos/

### Mac:
  - Installation instructions: https://store.docker.com/editions/community/docker-ce-desktop-mac

### Others:
  - https://www.docker.com/community-edition#/download
  
# System Prerequisites

|OS| Prerequisite |
|--|:--|
|CentOS|7.x (64-bit)|
|RedHat|RHEL 7.x (64-bit)|
|Debian|Stretch, Jessie 8.0, Wheezy 7.7 (64-bit)|
|Fedora|Fedora 25, Fedora 24 (64-bit)|
|Ubuntu|Zesty 17.04 (LTS),Yakkety 16.10, Xenial 16.04 (LTS),Trusty 14.04 (LTS)|
|OSX|Yosemite 10.10.3 or above|
|MS|Windows 10 Professional or Enterprise (64-bit)|

# Getting SwarmDB Docker

### Download the docker image:

      $ sudo docker pull wolkinc/swarmdb

### Deploy the docker image:

      $ sudo docker run --name=swarmdb --rm -it -p 2001:2001 -p 8501:8501 wolkinc/swarmdb

### Port Mapping:

| Ports | Descriptions |
|--|--|
| 2001:2001 | <http_system_port>:<http_container_port> |
| 8501:8501 | <swarmDB_system_port>:<swarmDB_container_port> |

## Verify SwarmDB

Deploying the image above will run SWARMDB in the Docker container. To verify if SWARMDB is running:

    $ ps aux | grep wolkdb | grep -vE 'wolkdb-start|grep'

## SwarmDB Configurations 

### To start swarmDB with the default config, run this on the command line:

      $ /usr/local/swarmdb/bin/wolkdb 
 
(Please note: Docker will automatically start wolkdb. So no need to run the above command unless you stopped it for development purposes.)

### To start swarmDB `IN THE BACKGROUND` with the default config, run this on the command line:

      $ /usr/local/swarmdb/bin/wolkdb &
      
### To start swarmDB with a modified config file located in a different location:
        
      $ /usr/local/swarmdb/bin/wolkdb -config /path/to/swarmDB/config/file


### To start swarmDB `IN THE BACKGROUND` with a modified config file located in a different location:
        
      $ /usr/local/swarmdb/bin/wolkdb -config /path/to/swarmDB/config/file &
      
### To see the `wolkdb` options:
      
      $ /usr/local/swarmdb/bin/wolkdb -h

      Usage of /usr/local/swarmdb/bin/wolkdb:
      -config string
    	      Full path location to SWARMDB configuration file. (default "/usr/local/swarmdb/etc/swarmdb.conf")
      -loglevel int
    	Log Level Verbosity 1-6 (4 for debug) (default 3)
      -v	Prints current SWARMDB version

### The default swarmDB configuration file
    
      {
          "address": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", //For Example: "db4db066584dea75f4838c08ddfadc195225dd80"
          "authentication": 1,
          "chunkDBPath": "/usr/local/swarmdb/data",
          "currency": "WLK",
          "listenAddrHTTP": "0.0.0.0",
          "listenAddrTCP": "0.0.0.0",
          "portHTTP": 8501,
          "portTCP": 2001
          "privateKey": "ABCD....WXYZ", //For Example: "98b5321e784dde6357896fd20f13ac6731e9b1ea0058c8529d55dde276e45624"
          "targetCostBandwidth": 3.14159,
          "targetCostStorage": 2.71828,
          "users": [
              {
                  "address": "wxyz....abcd", //For Example: "db4db066584dea75f4838c08ddfadc195225dd80"
                  "autoRenew": 1,
                  "maxReplication": 5,
                  "minReplication": 3,
                  "passphrase": "wolk"
               }
          ],
          "usersKeysPath": "/usr/local/swarmdb/data/keystore"
      }
      

#### Modifying the SwarmDB configuration:
You can add new items in the `users` array and make sure to restart swarmDB after modifying the configuration file.

##### To restart:
###### 1. Kill the process named `wolkdb` 
       $ sudo kill -9 $(ps aux | grep wolkdb | grep -v grep | awk '{print$2}')
      
###### 2. Start `wolkdb`:
        $ /usr/local/swarmdb/bin/wolkdb &
      

#  Interfaces
See our [Wiki](https://github.com/wolktoken/swarm.wolk.com/wiki) for [Node.js](https://github.com/wolktoken/swarm.wolk.com/wiki/2.-Node.js-Interface), [Go](https://github.com/wolktoken/swarm.wolk.com/wiki/3.-Go-Interface), [Http](https://github.com/wolktoken/swarm.wolk.com/wiki/5.-HTTP-Interface), and [Command Line Interface](https://github.com/wolktoken/swarm.wolk.com/wiki/4.-CLI).
