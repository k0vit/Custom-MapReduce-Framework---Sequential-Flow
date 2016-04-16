package neu.edu.mapreduce.master;

import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

import neu.edu.mapreduce.common.Node;

public class Master {

	private Job job;
	private Properties clusterProperties;
	private List<Node> slaves;

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

		setup();

		return true;
	}

	private void setup() {
		readClusterProperties();
		readInstanceDetails();
		readConfiguration();
		uploadConfiguration();
	}

	private void uploadConfiguration() {
		// TODO Auto-generated method stub
		
	}

	private void readInstanceDetails() {
		// TODO Auto-generated method stub
		
	}

	private void readClusterProperties() {
		// TODO Auto-generated method stub
		
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
