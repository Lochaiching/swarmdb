

# How to Install SwarmDB

# Install Docker CE (Community Edition)
https://www.docker.com/community-edition#/download

* CentOS:
  - Installation instructions: https://docs.docker.com/engine/installation/linux/docker-ce/centos/

* Mac:
  - Installation instructions: https://store.docker.com/editions/community/docker-ce-desktop-mac

* Others:
  - https://www.docker.com/community-edition#/download
  
# Prerequisites
* CentOS 7.x 64-bit.
* Red Hat Enterprise Linux (RHEL) 7.x 64-bit.

* Debian 64-bit:
Debian stretch (testing),
Debian Jessie 8.0,
Debian Wheezy 7.7.

* Fedora 64-bit:
Fedora 25,
Fedora 24.

* Ubuntu versions:
Zesty 17.04 (LTS),
Yakkety 16.10,
Xenial 16.04 (LTS),
Trusty 14.04 (LTS).

* MAC OSX Yosemite 10.10.3 or above.
* MS Windows 10 Professional or Enterprise 64-bit.

# Getting SwarmDB Docker

* Download the docker image:
  - `$ sudo docker pull wolkinc/wolknode`

* Deploy the docker image:
  - `$ sudo docker run --name=wolknode --rm -it -p 8500:8500 -p 5001:5000 -p 30303:30303 -p 30399:30399 -p 30303:30303/udp -p 30399:30399/udp wolkinc/wolknode`

* Port Mapping:

| Ports | Descriptions |
|--|--|
| 8500:8500 | <swarm_http_system_port>:<swarm_http_container_port> |
| 5001:5000 |  <syslog_system_port>:<syslog_container_port> |
| 30303:30303 | <geth_tcp_system_port>:<geth_tcp_container_port> |
| 30399:30399 | <swarm_tcp_system_port>:<swarm_tcp_container_port> |
| 30303:30303/udp | <geth_udp_system_port>:<geth_udp_container_port> |
| 30399:30399/udp | <swarm_udp_system_port>:<swarm_udp_container_port> |
| 30301:30301/udp | *<bootnode_udp_system_port>:<bootnode_udp_container_port> (*Not used here) |

# Running SwarmDB

Deploying the image above will run GETH and SWARM in the Docker container. To verify if GETH and SWARM are running:
  - `$ ps aux | grep -E 'geth|swarm' | grep -v grep`

## Configurations 

* To check current geth account:
  - `$ geth attach $DATADIR/geth.ipc --exec eth.accounts`

* To create new geth account:
  - `$ geth --datadir $DATADIR account new`

* Note: If you downloaded our docker image using instructions above, $DATADIR will point to: `/var/www/vhosts/data`. To check what your DATADIR is, run:
  - `$ echo $DATADIR`  

#  Interfaces

See [SwarmDB Wiki](https://github.com/wolktoken/swarm.wolk.com/wiki) for more
