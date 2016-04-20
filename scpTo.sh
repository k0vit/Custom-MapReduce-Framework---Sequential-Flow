#!/bin/bash

chmod 400 ec2key.pem

while IFS="=" read key value
do
	key="${key// }"
        value="${value// }"

        if [ "$key" == "Jar" ]
        then
                jar=$value
 	fi
done < user.properties

echo $jar

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type S/C

while IFS="," read II PRIP PUIP NT
do
	scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" mapreduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar ec2-user@$PUIP:~/

	scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" $jar ec2-user@$PUIP:~/

	scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" cluster.properties ec2-user@$PUIP:~/

	scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" instancedetails.csv ec2-user@$PUIP:~/
done < instancedetails.csv
