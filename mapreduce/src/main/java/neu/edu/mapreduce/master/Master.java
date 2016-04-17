package neu.edu.mapreduce.master;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileNames.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static spark.Spark.post;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.mapreduce.common.Node;
import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3File;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

public class Master {

	private static final Logger log = Logger.getLogger(Master.class.getName());
	private Job job;
	private Properties clusterProperties;
	private List<Node> nodes;
	private S3Wrapper s3wrapper;
	private int slaveCount = 0;
	
	private static AtomicInteger noOfMapperDone = new AtomicInteger(0); 

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

	/**
	 * Step 1
	 */
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

	/**
	 * Step 2
	 */
	private void startJob() {
		for (Node node: nodes) {
			if (node.isSlave()) {
				NodeCommWrapper.sendData(node.getPrivateIp(), START_JOB_URL);
				slaveCount++;
			}
		}
	}

	/**
	 * Step 3
	 */
	private void sendFilesToMapper() {
		List<S3File> s3Files = s3wrapper.getListOfObjects(job.getConfiguration().get(INPUT_PATH));
		Collections.sort(s3Files);

		List<NodeToFile> nodeToFile = new ArrayList<>(nodes.size()); 
		for (Node node : nodes) {
			nodeToFile.add(new NodeToFile(node));
		}

		for (S3File file : s3Files) {
			if (file.getFileName().endsWith(".gz")) {
				nodeToFile.get(0).addToFileNameLst(file.getFileName());
				nodeToFile.get(0).addToTotalSize(file.getSize());
			}
			Collections.sort(nodeToFile);
		}

		for (NodeToFile node : nodeToFile) {
			NodeCommWrapper.sendData(node.getNode().getPrivateIp(), FILE_URL, node.getFileNameLst());
		}
	}
	
	private void listenToEndOfMapper() {
		post(EOM_URL, (request, response) -> {
			response.status(200);
			response.body("SUCCESS");
			noOfMapperDone.incrementAndGet();
			return response.body().toString();
		});
		
		while (noOfMapperDone.get() != slaveCount) {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// TODO
			}
		}
	}
}

class NodeToFile implements Comparable<NodeToFile>{
	private Node node;
	private Long totalSize = new Long(0);
	private StringBuilder fileNameLst = new StringBuilder();

	public NodeToFile(Node node) {
		this.node = node;
	}

	public Node getNode() {
		return node;
	}

	public Long getTotalSize() {
		return totalSize;
	}

	public void addToTotalSize(Long totalSize) {
		this.totalSize += totalSize;
	}

	public String getFileNameLst() {
		fileNameLst.deleteCharAt(fileNameLst.length() - 1);
		return fileNameLst.toString();
	}

	public void addToFileNameLst(String fileName) {
		fileNameLst.append(fileName).append(",");
	}

	@Override
	public int compareTo(NodeToFile o) {
		return totalSize.compareTo(o.getTotalSize());
	}
}