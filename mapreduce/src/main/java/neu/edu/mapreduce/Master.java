package neu.edu.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class Master {
	
	Job job;

	public Master(Job job) {
		this.job = job;
	}

	/**
	 * Read config
	 * upload config
	 * download InstanceDetails.csv 
	 * download cluster.properties
	 * 
	 * "/start" - for a new job 
	 * 
	 * get the Input path and read the files and divide by slaves
	 *
	 * @return
	 */
	public boolean submit() {
		
		
		return true;
	}
}
