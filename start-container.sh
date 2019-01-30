#!/bin/bash

# the number of slave
N=$1
if [ $# = 0 ]
then
	echo "Please specify the slave node number"
	exit 1
fi

# create network
sudo docker network create --driver=bridge clusterHadoop

# start hadoop master container
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=clusterHadoop \
                -p 50070:50070 \
                -p 8088:8088 \
		-p 7077:7077 \
		-p 16010:16010 \
                --name hadoop-master \
                --hostname hadoop-master \
                spark-hadoop:latest &> /dev/null


# start hadoop slave container
i=1
while [ $i -lt $((N + 1)) ]
do
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
	port=$(( 8040 + $i ))
	sudo docker run -itd \
			-p $port:8042 \
	                --net=clusterHadoop \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                spark-hadoop:latest &> /dev/null
	i=$(( $i + 1 ))
done 

# get into hadoop master container
sudo docker exec -it hadoop-master bash

# start hadoop cluster
sudo ./start-hadoop.sh
