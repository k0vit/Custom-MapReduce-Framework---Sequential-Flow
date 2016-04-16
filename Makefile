dist_sort:
	rm -rf ec2key.pem
	rm -rf InstanceDetails.csv
	rm -rf sortnode-*.txt
	rm -rf client-output.txt
	rm -rf cluster_manager*.txt
	rm -rf *.jar
	cd ClusterManager && exec mvn clean install
	cp ClusterManager/target/nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cd SortNode && exec mvn clean install
	cp SortNode/target/SortNode-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cd Client && exec mvn clean install
	cp Client/target/client-0.0.1-SNAPSHOT-jar-with-dependencies.jar .	
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar create > cluster_manager_create.txt 2>&1
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar start > cluster_manager_start.txt 2>&1
	chmod 777 scpTo.sh
	sleep 30
	./scpTo.sh
	chmod 777 sshToRun.sh
	./sshToRun.sh $(INPUT_PATH) $(OUTPUT_PATH)
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar terminate $(OUTPUT_PATH) > cluster_manager_terminate.txt 2>&1

terminate:
	java -jar nisar_shah_lai_samant_a9-0.0.1-SNAPSHOT-jar-with-dependencies.jar terminate $(OUTPUT_PATH) > cluster_manager_terminate.txt 2>&1
