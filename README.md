


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

      $ sudo docker run --name=swarmdb --rm -it -p 2001:2001 -p 5001:5000 -p 8501:8501 wolkinc/swarmdb

### Port Mapping:

| Ports | Descriptions |
|--|--|
| 2001:2001 | <http_system_port>:<http_container_port> |
| 5001:5000 |  <syslog_system_port>:<syslog_container_port> |
| 8501:8501 | <swarmDB_system_port>:<swarmDB_container_port> |

# Running SwarmDB

Deploying the image above will run SWARMDB in the Docker container. To verify if SWARMDB is running:

    $ ps aux | grep main | grep -v grep

## SwarmDB Configurations 

### To start swarmDB with the default config, run this on the command line:

      $ /swarmdb/bin/main

### To start swarmDB with a modified config file located in a different location:
        
      $ /swarmdb/bin/main -config /path/to/swarmDB/config/file

### To see the `main` options:
      
      $ /swarmdb/bin/main -h

      Usage of /swarmdb/bin/main:
      -config string
    	Full path location to SWARMDB configuration file. (default "/swarmdb/swarmdb.conf")

### The default swarmDB configuration file
    
      {
          "listenAddrTCP": "0.0.0.0",
          "portTCP": 2001
          "listenAddrHTTP": "0.0.0.0",
          "portHTTP": 8501,
          "address": "9982ad7bfbe62567287dafec879d20687e4b76f5",
          "privateKey": "4b0d79af51456172dfcc064c1b4b8f45f363a80a434664366045165ba5217d53",
          "chunkDBPath": "/swarmdb/data/keystore",
          "authentication": 1,
          "usersKeysPath": "/swarmdb/data/keystore",
          "users": [{
              "address": "9982ad7bfbe62567287dafec879d20687e4b76f5",
              "passphrase": "wolkwolkwolk",
              "minReplication": 3,
              "maxReplication": 5,
              "autoRenew": 1
           }],
          "currency": "WLK",
          "targetCostStorage": 2.71828,
          "targetCostBandwidth": 3.14159
      }
      

#### Modifying the SwarmDB configuration:
You can add new items in the `users` array and make sure to restart swarmDB after modifying the configuration file.

##### To restart:
###### 1. Kill the process named `main` 
       $ sudo kill -9 `ps aux | grep main | grep -v grep | awk '{print$2}'
      
###### 2. Start `main`:
        $ /swarmdb/bin/main -config /path/to/swarmDB/config/file &
      

#  Interfaces
See our [Wiki](https://github.com/wolktoken/swarm.wolk.com/wiki) for [Node.js](https://github.com/wolktoken/swarm.wolk.com/wiki/2.-Node.js-Interface), [Go](https://github.com/wolktoken/swarm.wolk.com/wiki/3.-Go-Interface), [Http](https://github.com/wolktoken/swarm.wolk.com/wiki/5.-HTTP-Interface), and [Command Line Interface](https://github.com/wolktoken/swarm.wolk.com/wiki/4.-CLI).
