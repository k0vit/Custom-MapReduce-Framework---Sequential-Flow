#!/bin/bash

chmod 400 ec2key.pem

# II - instance identifier
# PRIP - private ip
# PUIP - public ip
# NT - Node Type S/C

while IFS="," read II PRIP PUIP NT
do
        if [ "$NT" == "C" ]
        then
                scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" client-0.0.1-SNAPSHOT-jar-with-dependencies.jar ec2-user@$PUIP:~/
        else
                scp -i ec2key.pem -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" SortNode-0.0.1-SNAPSHOT-jar-with-dependencies.jar ec2-user@$PUIP:~/
        fi
done < InstanceDetails.csv
