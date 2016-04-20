#!/bin/bash

chmod 400 ec2key.pem

while IFS="=" read key value
do
        #key="${key// }"
        #value="${value// }"

        if [ "$key" == "Arguments" ]
        then
                args=$value
	elif [ "$key" == "Main" ]
        then
               main=$value
        fi
done < user.properties

echo $args
echo $main

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type Master/Slave

while IFS="," read II PRIP PUIP NT
do
        if [ "$NT" == "M" ]
        then
                masterip=$PUIP
        else
                ssh -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" ec2-user@$PUIP "java -cp ~/*:. neu.edu.mapreduce.slave.Slave 2>&1" > slave-$PUIP-output.txt &
        fi
done < instancedetails.csv

ssh -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" ec2-user@$masterip "java -cp ~/*:. $main $args 2>&1" > master-$masterip-output.txt
