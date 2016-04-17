package neu.edu.mapreduce.slave;

import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multi.DownloadPackage;
import org.jets3t.service.multi.SimpleThreadedStorageService;
import org.jets3t.service.security.AWSCredentials;

import com.amazonaws.auth.BasicAWSCredentials;

import neu.edu.mapreduce.common.Node;
import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 * download cluster.properties download InstanceDetails.csv
 * 
 * listen to "/start" download Config Instantiate SlaveJob call start
 * 
 * listen on /file
 * 
 * 
 * 
 * 
 * @author kovit
 *
 */
public class Slave {
	private static final Logger log = Logger.getLogger(Slave.class.getName());
	private static final String port = "4567";
	private static final String endOfMap = "/EOM";
	private static final String endOfReducer = "/EOR";
	private static String MASTER_IP;
	public static String JOB_NAME;
	public static String MAPPER_CLASS;
	public static String REDUCER_CLASS;
	public static String OUTPUT_KEY_CLASS;
	public static String OUTPUT_VALUE_CLASS;
	public static String MAP_OUTPUT_KEY_CLASS;
	public static String MAP_OUTPUT_VALUE_CLASS;
	public static String INPUT_PATH;
	public static String OUTPUT_PATH;
	public static String JAR_BY_CLASS;
	public static String BUCKET_NAME;
	public static String ACCESS_KEY;
	public static String SECRET_KEY;

	public static void main(String[] args) {
		initialiseAllProperties();

		List<Node> instanceNodes = Utilities.readInstanceDetails();

		ReceiveFilesFromMaster();

		ReceiveKeysFromMaster();
	}

	private static void initialiseAllProperties() {
		setJetS3TProperties();
		Properties properties = Utilities.readClusterProperties();
		BUCKET_NAME = properties.getProperty("Bucket");
		ACCESS_KEY = properties.getProperty("AccessKey");
		SECRET_KEY = properties.getProperty("SecretKey");

		AWSCredentials awsCred = new AWSCredentials(ACCESS_KEY, SECRET_KEY);
		S3Service s3Service = new RestS3Service(awsCred);

		try {
			S3Object config = s3Service.getObject(BUCKET_NAME, "configuration.prop");
			Properties configuration = new Properties();
			configuration.load(new InputStreamReader(config.getDataInputStream()));
			JOB_NAME = configuration.getProperty("jobName");
			MAPPER_CLASS = configuration.getProperty("mapperClass");
			REDUCER_CLASS = configuration.getProperty("reducerClass");
			OUTPUT_KEY_CLASS = configuration.getProperty("outputKeyClass");
			OUTPUT_VALUE_CLASS = configuration.getProperty("outputValueClass");
			MAP_OUTPUT_KEY_CLASS = configuration.getProperty("mapOutputKeyClass");
			MAP_OUTPUT_VALUE_CLASS = configuration.getProperty("mapOutputValueClass");
			INPUT_PATH = configuration.getProperty("inputPath");
			OUTPUT_PATH = configuration.getProperty("outputPath");
			JAR_BY_CLASS = configuration.getProperty("jarByClass");
		} catch (ServiceException e) {
			log.severe("S3 service is unable to List the S3 bucket: " + BUCKET_NAME);
		} catch (IOException e) {
			log.severe("Cannot load inputstream from the S3 object");
		}

	}

	private static void setJetS3TProperties() {
		// Load your default settings from jets3t.properties file on the
		// classpath
		Jets3tProperties myProperties = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);

		// Override default properties (increase number of connections and
		// myProperties.setProperty("httpclient.max-connections", "100");
		myProperties.setProperty("threaded-service.max-thread-count", "8");
		myProperties.setProperty("threaded-service.admin-max-thread-count", "8");
		myProperties.setProperty("s3service.max-thread-count", "8");
		log.info("Set the JetS3t multithreading properties");
	}

	private static void ReceiveFilesFromMaster() {
		post("/files", (request, response) -> {
			log.info("Received files from Master for downloading");
			MASTER_IP = request.ip();
			String files = request.body();
			String[] filenames = files.split(",");
			BasicAWSCredentials awsCred = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
			for (String filename : filenames) {
				// Download each file received.
				String localFile = S3Wrapper.downloadAndStoreFileInLocal(filename, awsCred, BUCKET_NAME);

				try (FileInputStream fis = new FileInputStream(localFile);
						BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
					String line = null;
					while ((line = br.readLine()) != null) {
						// Go through each line. and call mapper instance.
					}
				}

			}

			log.info("All files done..Sending end of mapper to Master");
			NodeCommWrapper.sendData(MASTER_IP, port, endOfMap, "DONE");
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});
	}

	private static void ReceiveKeysFromMaster() {
		post("keys", (request, response) -> {
			log.info("Received keys from Master..Keys: " + request.body());
			String keys = request.body();
			String[] keySplit = keys.split(",");

			for (String key : keySplit) {
				downloadBucketIntoLocal(key);
				List<String> allKeyData = new ArrayList<String>();
				File[] files = listDirectory(key);
				for (File file : files) {
					List<String> fileData = FileUtils.readLines(file, "UTF-8");
					allKeyData.addAll(fileData);
				}
				Iterator<String> fullDataIterable = allKeyData.iterator();
				// Pass it to reducer.
			}

			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});

		log.info("All files done..Sending end of reducer to Master");
		NodeCommWrapper.sendData(MASTER_IP, port, endOfReducer, "DONE");
	}

	private static void downloadBucketIntoLocal(String key) {
		AWSCredentials awsCred = new AWSCredentials(ACCESS_KEY, SECRET_KEY);
		S3Service s3Service = new RestS3Service(awsCred);
		S3Bucket s3Bucket;
		try {
			s3Bucket = s3Service.getBucket(key);
			S3Object[] bucketFiles = s3Service.listObjects(s3Bucket.getName());
			SimpleThreadedStorageService simpleMulti = new SimpleThreadedStorageService(s3Service);
			DownloadPackage[] downloadPackages = new DownloadPackage[bucketFiles.length];
			for (int i = 0; i < downloadPackages.length; i++) {
				downloadPackages[i] = new DownloadPackage(bucketFiles[i], new File(bucketFiles[i].getKey()));
			}
			simpleMulti.downloadObjects(key, downloadPackages);
		} catch (ServiceException e) {
			log.severe("Service exception connected to S3: Exception: " + e.getMessage());
		}

	}

	/**
	 * List a given folder.
	 * 
	 * @param directoryPath
	 * @return
	 */
	public static File[] listDirectory(String directoryPath) {
		log.info("Listing folder: " + directoryPath);
		File directory = new File(directoryPath);
		File[] files = directory.listFiles();
		return files;
	}

}
