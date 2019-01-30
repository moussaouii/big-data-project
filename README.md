# Hadoop-Spark cluster in Docker


These containers were originally from [here](https://github.com/kiwenlau/hadoop-cluster-docker)

This project is a  cluster of  containers  with as platforms installed
* Apache Hadoop Version: 2.7.2
* Apache Spark Version: 2.2.1

This cluster has a master and several slaves

 
## Getting Started

### Prerequisites

You need Docker to run this project on your local machine.


## Running the project


##### 1. Build the Docker image from the Dockerfile file

```
./build-image.sh
```

##### 2. start container

```
sudo ./start-container.sh 2
```
- Here 2 is parameter it's the number of slave nodes.
- start 3 containers with 1 master and 2 slaves
- you will get into the /root directory of hadoop-master container 

**output:**

```
start hadoop-master container...
start hadoop-slave1 container...
start hadoop-slave2 container...
root@hadoop-master:~# 
```


##### 3. Launch the yarn and hdfs daemons by running:

```
sudo ./start-hadoop.sh
```

**output:**

```
Starting namenodes on [hadoop-master]
hadoop-master: Warning: Permanently added 'hadoop-master,172.27.0.2' (ECDSA) to the list of known hosts.
hadoop-master: starting namenode, logging to /usr/local/hadoop/logs/hadoop-root-namenode-hadoop-master.out
hadoop-slave2: Warning: Permanently added 'hadoop-slave2,172.27.0.4' (ECDSA) to the list of known hosts.
hadoop-slave1: Warning: Permanently added 'hadoop-slave1,172.27.0.3' (ECDSA) to the list of known hosts.
hadoop-slave2: starting datanode, logging to /usr/local/hadoop/logs/hadoop-root-datanode-hadoop-slave2.out
hadoop-slave1: starting datanode, logging to /usr/local/hadoop/logs/hadoop-root-datanode-hadoop-slave1.out
Starting secondary namenodes [0.0.0.0]
0.0.0.0: Warning: Permanently added '0.0.0.0' (ECDSA) to the list of known hosts.
0.0.0.0: starting secondarynamenode, logging to /usr/local/hadoop/logs/hadoop-root-secondarynamenode-hadoop-master.out


starting yarn daemons
starting resourcemanager, logging to /usr/local/hadoop/logs/yarn--resourcemanager-hadoop-master.out
hadoop-slave1: Warning: Permanently added 'hadoop-slave1,172.27.0.3' (ECDSA) to the list of known hosts.
hadoop-slave2: Warning: Permanently added 'hadoop-slave2,172.27.0.4' (ECDSA) to the list of known hosts.
hadoop-slave1: starting nodemanager, logging to /usr/local/hadoop/logs/yarn-root-nodemanager-hadoop-slave1.out
hadoop-slave2: starting nodemanager, logging to /usr/local/hadoop/logs/yarn-root-nodemanager-hadoop-slave2.out
```

##### 4. Load the file "ratings.dat" into the directory datain hdfs:

```
hadoop fs –mkdir -p data
hadoop fs –put ratings.dat data
```


##### 5. run application:

```
./start-app.sh
```


## Author

**Ismail MOUSSAOUI** 
**Youssef ZMAROU**





