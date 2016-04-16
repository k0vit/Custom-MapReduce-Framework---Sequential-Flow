package neu.edu.mapreduce.master;

import java.io.PrintWriter;

import org.apache.hadoop.mapreduce.Job;

public class Master {

	Job job;

	public Master(Job job) {
		this.job = job;
	}

	/**
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

		readConfiguration();

		return true;
	}

	private void readConfiguration() {
		try {
			PrintWriter writer = new PrintWriter("Configuration.properties");
			for (String key : job.getConfiguration().getMap().keySet()) {
				StringBuilder sb = new StringBuilder(key)
				.append("=").append(job.getConfiguration().get(key));
				writer.println(sb.toString());
			}
			writer.close();
		}
		catch (Exception e) {
			// TODO
		}
	}
}
