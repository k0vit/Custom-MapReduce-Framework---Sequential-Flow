package neu.edu.mapreduce.master;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileNames.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;

import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.mapreduce.common.Node;
import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3File;
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
	 * Master working:-
	 * 
	 * 1) 
	 * Read instancedetails.csv and cluster.properties
	 * Read job configuration 
	 * Upload configuration file to s3 at Bucket\Configuration.properties
	 * 
	 * 2) 
	 * "/start" - for a new job (supporting multiple jobs)
	 * 
	 * 3) 
	 * Get the Input path and read the files and divide by #slaves or file size
	 * send the files on /files
	 * 
	 * 4)
	 * listen to /EOM meaning end of mapper
	 *  
	 * 5)
	 * check if all mapper are done
	 * once all mapper are done download keys from s3
	 * divide keys by #slaves or key file size
	 * send keys on /keys to mapper
	 * 
	 * 6)
	 * listen to /EOR mean end of reducer
	 * once all reducer are done return true
	 * 
	 * @return 
	 * 		true if job completed successfully else false
	 */
	public boolean submit() {

		setup();
		startJob();
		sendFilesToMapper();
		listenToEndOfMapper();
		sendKeysToReducer();
		listenToEndOfReducer();

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

	private void startJob() {
		for (Node node: nodes) {
			if (node.isSlave()) {
				NodeCommWrapper.sendData(node.getPrivateIp(), START_JOB_URL);
			}
		}
	}
	
	private void sendFilesToMapper() {
		List<S3File> s3Files = s3wrapper.getListOfObjects(job.getConfiguration().get(INPUT_PATH));
		for (S3File file : s3Files) {
			if (file.getFileName().endsWith(".gz")) {
				
			}
		}
	}
}
