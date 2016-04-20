run:
	rm -rf ec2key.pem
	rm -rf instancedetails.csv
	rm -rf slave-*.txt
	rm -rf master-output.txt
	rm -rf cluster_manager*.txt
	#rm -rf *.jar
	#cd clustermanager && exec mvn clean install -DskipTests
	#cp clustermanager/target/cluster_manager-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	#cd mapreduce && exec mvn clean install -DskipTests
	#cp mapreduce/target/mapreduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar .	
	java -jar cluster_manager-0.0.1-SNAPSHOT-jar-with-dependencies.jar create > cluster_manager_create.txt 2>&1
	chmod 777 scpTo.sh
	./scpTo.sh
	chmod 777 sshToRun.sh
	./sshToRun.sh
	java -jar cluster_manager-0.0.1-SNAPSHOT-jar-with-dependencies.jar terminate > cluster_manager_terminate.txt 2>&1

terminate:
	java -jar cluster_manager-0.0.1-SNAPSHOT-jar-with-dependencies.jar terminate > cluster_manager_terminate.txt 2>&1
