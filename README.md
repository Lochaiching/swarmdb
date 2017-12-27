
# How to Install SWARMDB

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

# Get SWARMDB Docker

Download the docker image:
* $ sudo docker pull wolkinc/wolkmain

Deploy the docker image:
* $ sudo docker run --name=wolkmain --rm -it -p 8500:8500 -p 5001:5000 -p 30303:30303 -p 30399:30399 -p 30301:30301/udp -p 30303:30303/udp -p 30399:30399/udp wolkinc/wolkmain

# Run SWARMDB

Test if it works

## Configuration 

Under construction

#  Interfaces

See the https://github.com/wolktoken/swarm.wolk.com/wiki for more
