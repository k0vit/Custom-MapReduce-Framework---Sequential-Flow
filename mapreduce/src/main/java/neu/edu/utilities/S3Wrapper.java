package neu.edu.utilities;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.opencsv.CSVWriter;

/**
 * Create a wrapper class to access S3 functions.
 * 
 */
public class S3Wrapper {
	public S3Wrapper(AmazonS3 s3client) {
		this.s3client = s3client;
	}

	/**
	 * List objects of the given path.
	 * 
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public List<String> getListOfObjects(String bucketName, String prefix) {
		log.info(String.format("Requesting object listing for s3://%s/%s", bucketName, prefix));

		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);

		ArrayList<String> files = new ArrayList<String>();
		ObjectListing listing = null;
		do {
			listing = s3client.listObjects(request);
			for (S3ObjectSummary summary : listing.getObjectSummaries()) {
				log.info("Object: " + summary.getKey());
				files.add(summary.getKey());
			}
		} while (listing.isTruncated());

		return files;
	}

	/**
	 * Obtain an input stream to read from given the path to S3.
	 * 
	 * @param bucketName
	 * @param objectId
	 * @return
	 */
	public InputStream getObjectInputStream(String bucketName, String objectId) {
		log.info(String.format("Requesting for object: s3://%s/%s", bucketName, objectId));

		GetObjectRequest request = new GetObjectRequest(bucketName, objectId);
		S3Object object = s3client.getObject(request);
		ObjectMetadata md = object.getObjectMetadata();

		log.info(String.format("Content Type: %s", md.getContentType()));
		log.info(String.format("Content Length: %d", md.getContentLength()));

		return object.getObjectContent();
	}

	/**
	 * Download the given file to the outputpath specified.
	 * 
	 * @param outputPath
	 * @param cred
	 * @param filename
	 * @return
	 */
	public String readOutputFromS3(String outputPath, BasicAWSCredentials cred, String filename) {
		TransferManager tx = new TransferManager(cred);
		String simplifiedPath = (outputPath.replace("s3://", ""));
		int index = simplifiedPath.indexOf("/");
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info(String.format("[%s] Downloaded file with Bucket Name: %s Key: %s ", filename, bucketName, key));
		log.info("CURRENT USER DIRECTORY: " + System.getProperty("user.dir"));
		Download d = tx.download(bucketName, key, new File(filename));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed downloading the file " + filename + ". Reason " + e.getMessage());
		}
		tx.shutdownNow();
		return filename;
	}

	/**
	 * upload given file to the output path specified.
	 * 
	 * @param file
	 *            File to be uploaded.
	 * @param bucket
	 *            bucket name e.g. s3://kovit
	 * @return true if uploaded successfully.
	 */
	public boolean uploadFile(String file, String bucket) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(bucket);
		String remote = local.getName();
		try {
			s3client.putObject(new PutObjectRequest(folder, remote, local));
		} catch (Exception e) {
			log.severe("Failed to upload file: " + local.getName() + " :" + e.getMessage());
		}
		return true;
	}

	/**
	 * Upload data to the output path.
	 * 
	 * @param data
	 * @param outputPath
	 * @return
	 */
	public boolean uploadStringData(String data, String outputPath) {
		byte[] bytedata = data.getBytes();
		// InputStream is = new ByteArrayInputStream(bytedata);
		String simplifiedPath = (outputPath.replace("s3://", ""));
		int index = simplifiedPath.indexOf("/");
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info("Uploading file to bucket " + bucketName + " with key as " + key);
		TransferManager tx = new TransferManager(s3client);
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(bytedata.length);

		try (InputStream is = new ByteArrayInputStream(bytedata)) {
			Upload up = tx.upload(bucketName, key, is, meta);
			up.waitForCompletion();
		} catch (AmazonClientException | InterruptedException | IOException e) {
			log.severe("Failed uploading the file " + outputPath + ". Reason " + e.getMessage());
			return false;
		}
		log.info("Full file uploaded to S3 at the path: " + outputPath);
		return true;
	}

	/**
	 * Utility method.
	 * 
	 * @param path
	 * @return
	 */
	private static String removeS3(String path) {
		if (!path.startsWith("s3://"))
			return path;
		return path.substring("s3://".length());
	}

	/**
	 * Upload a list of records to the output path after creating a csv file.
	 * 
	 * @param outputS3Path
	 * @param nowSortedData
	 * @param instanceId
	 * @return
	 */
	public boolean uploadDataToS3(String outputS3Path, List<String[]> nowSortedData, long instanceId) {
		String fileName = "part-r-" + instanceId + ".csv";
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(fileName));
			writer.writeAll(nowSortedData);
			writer.close();
			if (uploadFile(fileName, outputS3Path)) {
				return true;
			}
		} catch (IOException e) {
			log.severe(e.getMessage());
		}
		return false;
	}

	/**
	 * DOwnload the file from the S3 path and store in local file system.
	 * 
	 * @param fileString
	 * @param awsCredentials
	 * @param inputS3Path
	 * @return
	 */
	public String downloadAndStoreFileInLocal(String fileString, BasicAWSCredentials awsCredentials,
			String inputS3Path) {
		String s3FullPath = inputS3Path + "/" + fileString;
		log.info(String.format("[%s] Downloading from s3 full path: %s", fileString, s3FullPath));
		readOutputFromS3(s3FullPath, awsCredentials, fileString);
		return fileString;
	}

	/**
	 * Upload the file to S3 using Transfer Manager.
	 * 
	 * @param outputS3Path
	 * @param nowSortedData
	 * @param instanceId
	 * @return
	 */
	public boolean uploadFileS3(String outputS3Path, List<String[]> nowSortedData, long instanceId) {
		TransferManager tx = new TransferManager(s3client);
		String fileName = "part-r-" + instanceId + ".csv";
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(fileName));
			writer.writeAll(nowSortedData);
			writer.close();
		} catch (IOException e) {
			log.severe("File not getting created. Reason: " + e.getMessage());
			return false;
		}
		String s3FullPath = outputS3Path + "/" + fileName;
		String simplifiedPath = (s3FullPath.replace("s3://", ""));
		int index = simplifiedPath.indexOf("/");
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		Upload up = tx.upload(bucketName, key, new File(fileName));
		try {
			up.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed downloading the file " + s3FullPath + ". Reason " + e.getMessage());
			return false;
		}
		log.info("Full file uploaded to S3 at the path: " + s3FullPath);
		return true;
	}

	private static final Logger log = Logger.getLogger(S3Wrapper.class.getName());
	private AmazonS3 s3client;

}
