package neu.edu.mr.manager;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * Uploads the InstanceDetails.csv
 * @author kovit
 *
 */
public class ClusterStarter {

	private ClusterParams params;
	private AmazonS3Client client;
	private final static Logger LOGGER = Logger.getLogger(Main.CLUSTER_MANAGER_LOGGER);

	public ClusterStarter(ClusterParams params) {
		client = new AmazonS3Client(new BasicAWSCredentials(params.getAccessKey(), params.getSecretKey()));
		client.setRegion(Region.getRegion(Regions.fromName("us-east-1")));
		this.params = params;
	}

	public boolean uploadToS3() {
		try {
			uploadFile(Main.CLUSTER_DETAILS_FILE_NAME);
			LOGGER.log(Level.FINE, Main.CLUSTER_DETAILS_FILE_NAME + " uploaded successfully");
		}
		catch (Exception e) {
			System.err.println("Failed to upload following files to s3 " + Main.CLIENT_JAR + ", " + Main.SORT_NODE_JAR
					+ ", " + Main.CLUSTER_DETAILS_FILE_NAME + ". Reason " + e.getMessage());
			return false;
		}
		return true;
	}

	boolean uploadFile(String file) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(params.getBucket());
		String bucket = folder;
		String remote = local.getName();
		client.putObject(new PutObjectRequest(bucket, remote, local));
		return true;
	}

	private static String removeS3(String path) {
		if (!path.startsWith("s3://"))
			return path;
		return path.substring("s3://".length());
	}
}
