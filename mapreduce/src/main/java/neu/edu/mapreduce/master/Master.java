package neu.edu.mapreduce.master;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.FileNames.JOB_CONF_PROP_FILE_NAME;

import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.mapreduce.common.Node;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

public class Master {

	private Job job;
	private Properties clusterProperties;
	private List<Node> nodes;
	private S3Wrapper s3wrapper;

	public Master(Job job) {
		this.job = job;
	}

	/**
	 * 
	 * Read job configuration 
	 * Upload configuration file to s3 at Bucket\Configuration.properties
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
		clusterProperties = Utilities.readClusterProperties();
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty("AccessKey"), clusterProperties.getProperty("SecretKey"))));
		nodes = Utilities.readInstanceDetails();
		readAndUploadConfiguration();
	}

	private void readAndUploadConfiguration() {
		try {
			PrintWriter writer = new PrintWriter(JOB_CONF_PROP_FILE_NAME);
			for (String key : job.getConfiguration().getMap().keySet()) {
				StringBuilder sb = new StringBuilder(key)
				.append("=").append(job.getConfiguration().get(key));
				writer.println(sb.toString());
			}
			writer.close();
			s3wrapper.uploadFile(JOB_CONF_PROP_FILE_NAME, clusterProperties.getProperty(BUCKET));
		}
		catch (Exception e) {
			// TODO
		}
	}
}
