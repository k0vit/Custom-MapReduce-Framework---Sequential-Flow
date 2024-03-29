package neu.edu.utilities;

import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.S3_URL;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Create a wrapper class to access S3 functions.
 * 
 * @author kovit nisar
 * @author naineel shah
 * 
 */
public class S3Wrapper {
	private static final Logger log = Logger.getLogger(S3Wrapper.class.getName());

	private AmazonS3 s3client;
	private TransferManager tx;

	public S3Wrapper(AmazonS3 s3client) {
		this.s3client = s3client;
		tx = new TransferManager(s3client);
	}

	/**
	 * @param s3InputPath
	 * 
	 * @return
	 * 		list of objects in that s3InputPath
	 */
	public List<S3File> getListOfObjects(String s3InputPath) {
		log.info("Getting list of objects from " + s3InputPath);
		String simplifiedPath = removeS3(s3InputPath);
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String prefix = simplifiedPath.substring(index + 1);
		return getListOfObjects(bucketName, prefix);
	}

	/**
	 * List s3 objects in the specified bucketname and prefix
	 */
	public List<S3File> getListOfObjects(String bucketName, String prefix) {
		log.info(String.format("Requesting object listing for s3://%s/%s", bucketName, prefix));

		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);

		List<S3File> s3Files = new ArrayList<S3File>();
		ObjectListing listing = s3client.listObjects(request);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();

		while (listing.isTruncated()) {
			listing = s3client.listNextBatchOfObjects(listing);
			summaries.addAll(listing.getObjectSummaries());
		}

		for (S3ObjectSummary summary : summaries) {
			s3Files.add(new S3File(summary.getKey(), summary.getSize()));
		}

		return s3Files;
	}

	/**
	 *	Downloads the file and stores it in the local dir specified at localFilePath 
	 */
	public String readOutputFromS3(String s3FileFullPath, String localFilePath) {
		String simplifiedPath = (s3FileFullPath.replace(S3_URL, ""));
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info(String.format("Downloading file with Bucket Name: %s Key: %s to local dir %s",
				bucketName, key, localFilePath));
		Download d = tx.download(bucketName, key, new File(localFilePath));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed downloading the file " + localFilePath + ". Reason " + e.getMessage());
		}
		log.info("Downloading completed successfully to " + localFilePath);
		return localFilePath;
	}

	/**
	 * upload given file to root dir i.e. bucket
	 * 
	 * @param file
	 *            File to be uploaded.
	 * @param bucket
	 *            bucket name e.g. s3://kovit
	 * @return true if uploaded successfully.
	 */
	public boolean uploadFileToBucket(String file, String bucket) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(bucket);
		String remote = local.getName();
		try {
			s3client.putObject(new PutObjectRequest(folder, remote, local));
			log.fine("Uploaded file " + file + " to s3 location " + bucket);
		} catch (Exception e) {
			log.severe("Failed to upload file: " + local.getName() + " :" + e.getMessage());
		}
		return true;
	}

	/**
	 * Utility method.
	 * 
	 * @param path
	 * @return
	 */
	private static String removeS3(String path) {
		if (!path.startsWith(S3_URL))
			return path;
		return path.substring(S3_URL.length());
	}

	/**
	 * DOwnload the file from the S3 path and store in local file system.
	 * 
	 * @param fileString
	 * @param awsCredentials
	 * @param inputDirS3Path
	 * @return
	 */
	public String downloadAndStoreFileInLocal(String inputDirS3Path, String fileString) {
		String s3FullPath = inputDirS3Path + S3_PATH_SEP + fileString;
		log.info(String.format("Downloading from s3 full path: %s to local dir %s", s3FullPath, fileString));
		readOutputFromS3(s3FullPath, fileString);
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
	public boolean uploadFileS3(String outputS3FullPath, File file) {
		String simplifiedPath = removeS3(outputS3FullPath);
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.fine("Uploading file " + file.getAbsolutePath() + " to bucket " + bucketName + " with key as " + key);
		Upload up = tx.upload(bucketName, key, file);
		try {
			up.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed uploading the file " + outputS3FullPath + ". Reason " + e.getMessage());
			return false;
		}
		log.fine("File uploaded to S3 at the path: " + outputS3FullPath);
		return true;
	}

	/**
	 * Downloads the entire dir
	 * 
	 * @param s3Path
	 * @param localDir
	 */
	public void downloadDir(String s3Path, String localDir) {
		String simplifiedPath = (s3Path.replace(S3_URL, ""));
		String bucketName = simplifiedPath.substring(0, simplifiedPath.indexOf(S3_PATH_SEP));
		String key = simplifiedPath.substring(simplifiedPath.indexOf(S3_PATH_SEP) + 1);
		log.info("Downloading file from " + bucketName + " and key as " + key + " to local dir " + localDir);
		MultipleFileDownload d = tx.downloadDirectory(bucketName, key, new File(localDir));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Downloading failed from " + s3Path + ". Reason: " + e.getMessage());
		}
		log.info("File downloaded successfully from " + s3Path + " to " + localDir);
	}

	public void shutDown() {
		tx.shutdownNow();
	}

	/**
	 * Deletes the entire directory
	 * @param s3DirPath
	 */
	public void deleteDir(String s3DirPath) {
		log.info("Deleting directory " + s3DirPath);
		List<S3File> files = getListOfObjects(s3DirPath);

		if (files == null || files.size() == 0) {
			log.info("Directory " + s3DirPath + " not found. Hence nothing to delete.");
			return;
		}

		String simplifiedPath = (s3DirPath.replace(S3_URL, ""));
		String bucketName = simplifiedPath.substring(0, simplifiedPath.indexOf(S3_PATH_SEP));

		log.info("There are " + files.size() + " object to be deleted");
		List<KeyVersion> keys = new ArrayList<KeyVersion>(60);
		for (S3File file: files) {
			keys.add(new KeyVersion(file.getFileName()));
			if (keys.size() == 1000) {
				log.info("Deleting 1000 objects from " + s3DirPath);
				deleteObjects(keys, bucketName);
			}
		}
		deleteObjects(keys, bucketName);
	}

	/**
	 * Deletes multiple object in s3
	 * @param keys
	 * @param bucketName
	 */
	private void deleteObjects(List<KeyVersion> keys, String bucketName) {
		DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName);
		multiObjectDeleteRequest.setKeys(keys);
		try {
			DeleteObjectsResult delObjRes = s3client.deleteObjects(multiObjectDeleteRequest);
			log.info(String.format("Successfully deleted all the %s items",delObjRes.getDeletedObjects().size()));
			keys.clear();

		} catch (MultiObjectDeleteException e) {
			log.severe(String.format("%s", e.getMessage()));
			log.severe(String.format("No. of objects successfully deleted = %s", e.getDeletedObjects().size()));
			log.severe(String.format("No. of objects failed to delete = %s", e.getErrors().size()));
			log.severe(String.format("Printing error data..."));
			for (DeleteError deleteError : e.getErrors()){
				log.severe(String.format("Object Key: %s\t%s\t%s", 
						deleteError.getKey(), deleteError.getCode(), deleteError.getMessage()));
			} 
		}
	}
}