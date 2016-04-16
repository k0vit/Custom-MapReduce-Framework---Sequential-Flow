#!/bin/bash

chmod 400 ec2key.pem

while IFS="=" read key value
do
	key="${key// }"
        value="${value// }"

        if [ "$key" == "AccessKey" ]
        then
                accesskey=$value
        elif [ "$key" == "SecretKey" ]
        then
                secretkey=$value
	elif [ "$key" == "Bucket" ]
        then
                clusterdetails=$value
		clusterdetails+="/InstanceDetails.csv"
        fi
done < cluster.properties

echo $accesskey
echo $secretkey
echo $clusterdetails

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type S/C

while IFS="," read II PRIP PUIP NT
do
        if [ "$NT" == "C" ]
        then
                clientip=$PUIP
        else
                ssh -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" ec2-user@$PUIP "java -jar ~/SortNode-0.0.1-SNAPSHOT-jar-with-dependencies.jar $1 $2 $clusterdetails $accesskey $secretkey 2>&1" > sortnode-$PUIP-output.txt &
        fi
done < InstanceDetails.csv

ssh -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" ec2-user@$clientip "java -jar ~/client-0.0.1-SNAPSHOT-jar-with-dependencies.jar $1 $2 $clusterdetails $accesskey $secretkey 2>&1" > client-output.txt
