


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
|OSX|Yosemite 10.11 or above|
|MS|Windows 10 Professional or Enterprise (64-bit)|

# Getting SwarmDB Docker

### Download the docker image:

      $ sudo docker pull wolkinc/swarmdb

### Deploy the docker container:

      $ sudo docker run --name=swarmdb --rm -it -p 2001:2001 -p 8501:8501 wolkinc/swarmdb

### Port Mapping:

| Ports | Descriptions |
|--|--|
| 2001:2001 | <http_system_port>:<http_container_port> |
| 8501:8501 | <swarmDB_system_port>:<swarmDB_container_port> |

### Detach/re-attach Docker container

#### In order to exit the Docker Container shell without killing the container, hit the following keys:
      $ ctrl + p + q

#### In order to re-attach to the swarmDB container
      $ docker attach $(docker ps | grep swarmdb | awk '{print$1}')

### To exit the container, type the following and press ENTER while you're in the container shell
      $ exit 13

### To clean the images (make sure you've exited the container using the above command. If not, the following command will throw error and will NOT be able to delete the images)
      $ docker rmi `docker images | grep swarmdb | awk '{print$3}'`

## Verify SwarmDB

Once the Docker IMAGE is deployed following above instructions, it will start the swarmDB process/service in the Docker container. To verify if swarmDB is running:

    $ ps aux | grep wolkdb | grep -vE 'wolkdb-start|grep'

## SwarmDB Configurations 

### To start swarmDB with the default config, run this on the command line:

      $ /usr/local/swarmdb/bin/wolkdb 
 
(Please note: Docker will automatically start the swarmdb/wolkdb. So no need to run the above command unless you stopped it for development purpose.)

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
`Default Location: /usr/local/swarmdb/etc/swarmdb.conf`

      {
          "address": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", //For Example: "db4db066584dea75f4838c08ddfadc195225dd80"
          "authentication": 1,
          "chunkDBPath": "/usr/local/swarmdb/data",
          "currency": "WLK",
          "listenAddrHTTP": "0.0.0.0",
          "listenAddrTCP": "0.0.0.0",
          "portHTTP": 8501,
          "portTCP": 2001
          "privateKey": "a1b2c3....d4e5f", //For Example: "98b5321e784dde6357896fd20f13ac6731e9b1ea0058c8529d55dde276e45624"
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
      
### swarmDB logging
        $ tail -f /usr/local/swarmdb/log/wolkdb.log

#### Modifying the SwarmDB configuration:
You can add new items in the `users` array and make sure to restart swarmDB after modifying the configuration file.

##### To restart:
###### 1. Kill the process named `wolkdb` 
       $ sudo kill -9 $(ps aux | grep wolkdb | grep -v grep | awk '{print$2}')
      
###### 2. Start `wolkdb`:
        $ /usr/local/swarmdb/bin/wolkdb &
      

#  Interfaces
See our [Wiki](https://github.com/wolktoken/swarm.wolk.com/wiki) & [DOCS](https://docs.wolk.com/) for [Node.js](https://docs.wolk.com/?javascript#), [Go](https://docs.wolk.com/?go#), [Http](https://docs.wolk.com/?plaintext#), and [Command Line Interface](https://docs.wolk.com/?javascript#),

