package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multi.DownloadPackage;
import org.jets3t.service.multi.SimpleThreadedStorageService;
import org.jets3t.service.security.AWSCredentials;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 * 1) 
 * listen on /Start
 * For each job there /Start will be called
 * Create a new Instance of SlaveJob and call the run
 * 
 * 2) 
 * Read cluster.properties
 * Download and Read configuration.properties
 * 
 * 3) 
 * listen to /file to start the mapper task
 *  
 * 4)
 * Create a folder called OutputOfMap 
 * for each file 
 * -- download the file
 * -- Instantiate the mapper class 
 * -- for each record in the file 
 * ---- call the map method 
 * -- once the file is done 
 * ---- call the close on Context to close all the FileWriter (check Context.write on Mapper below)
 * ---- upload the contents on s3
 * ---- delete the file
 * 
 * 5) 
 * call /EOM as mapper is done.
 * stop the routes /EOM and /File
 * 
 * 6) 
 * listen to /Key for the set of keys from the reducer
 * 
 * 7)
 * Create a folder Output
 * For each key 
 * -- Open file writer to file part-r-00<slaveId>-<file counter>
 * -- download the key directory from the s3
 * -- read all the files 
 * -- generate the iterator
 * -- Instantiate the reducer class
 * -- call the reduce method with the iterable
 * -- Close the file writer once the file is done
 * -- once done delete the key dir
 * 
 * 
 * 8)  
 * call /EOR 
 * once we get the response stop the spark java cluster
 * 
 * 
 * Context.write of Mapper [context.write(key, value)]
 * -- for each key 
 * ---- check if the key exist in the map maintained by the Context class [Map<String, FileWriter>]
 * ---- if the key is not present:
 * ------ create a dir called <key>_key_dir and create a file with <key>_timestamp_<slaveid> 
 * ------ open  FileWriter for that file and put in the map
 * ---- get the FileWriter from the map and write the record to it
 * 
 * 
 * 
 * Context.write of Reducer [contex.write(key, value)]
 * -- for each call write the record using the filewriter
 *  
 * @author kovit
 *
 */
public class Slave {
	private static final Logger log = Logger.getLogger(Slave.class.getName());
	
	public static void main() {
		/**
		 * step 1
		 */
		post(START_JOB_URL, (request, response) -> {
			response.status(200);
			response.body("SUCCESS");
			(new Thread(new SlaveJob())).start();
			return response.body().toString();
		});
	}
}

class SlaveJob implements Runnable {

	private static final Logger log = Logger.getLogger(SlaveJob.class.getName());
	
	private Properties clusterProperties;
	private String slaveId;
	private S3Wrapper s3wrapper;
	private Properties jobConfiguration;
	
	@Override
	public void run() {
		setup();
		ReceiveFilesFromMaster();
		ReceiveKeysFromMaster();
	}

	/**
	 * step 2
	 */
	private void setup() {
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty("AccessKey"), clusterProperties.getProperty("SecretKey"))));
		
		clusterProperties = Utilities.readClusterProperties();
		slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
		jobConfiguration = downloadAndReadJobConfig();
	}

	private Properties downloadAndReadJobConfig() {
		String s3FilePath = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + JOB_CONF_PROP_FILE_NAME;
		String localFilePath = s3wrapper.readOutputFromS3(s3FilePath, JOB_CONF_PROP_FILE_NAME);
		return Utilities.readPropertyFile(localFilePath);
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
