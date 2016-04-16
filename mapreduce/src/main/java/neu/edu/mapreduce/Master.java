package neu.edu.mapreduce;

import org.apache.hadoop.mapreduce.Job;

public class Master {
	
	Job job;

	public Master(Job job) {
		this.job = job;
	}

	/**
	 * download InstanceDetails.csv 
	 * download cluster.properties
	 * 
	 * Read config
	 * upload config
	 *
	 * 
	 * "/start" - for a new job 
	 * 
	 * get the Input path and read the files and divide by slaves
	 *
	 * @return
	 */
	public boolean submit() {
		
		downloadInstanceDetailsFile();
		downloadClusterProperties();
		
		
		return true;
	}

	private void downloadClusterProperties() {
		// TODO Auto-generated method stub
		
	}

	private void downloadInstanceDetailsFile() {
		// TODO Auto-generated method stub
		
	}
}
