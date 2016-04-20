package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.PART_FILE_PREFIX;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.JobConf.OUTPUT_PATH;
import static org.apache.hadoop.Constants.JobConf.REDUCER_OP_SEPARATOR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 *  Context.write of Reducer [contex.write(key, value)]
 * -- for each call write the record using the filewriter
 * 
 * @author kovit
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	private static final Logger log = Logger.getLogger(Reducer.class.getName());

	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		BufferedWriter bw;
		int counter = 0;
		private S3Wrapper s3wrapper;
		private Properties clusterProperties;
		private String slaveId;
		private String fileFullPath;

		public Context() {
			clusterProperties = Utilities.readClusterProperties();
			s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
					(clusterProperties.getProperty(ACCESS_KEY), clusterProperties.getProperty(SECRET_KEY))));
			slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
			log.info("Initializing reduce with slave id " + slaveId);
			
			initializeBufferedWriter();
		}

		private void initializeBufferedWriter() {
			fileFullPath = OP_OF_REDUCE + File.separator + PART_FILE_PREFIX + slaveId + counter;
			try {
				log.info("Creating file " + fileFullPath);
				bw = new BufferedWriter(new FileWriter(fileFullPath, true));
			} catch (IOException e) {
				log.severe("Failed to create BufferedWriter for file " + fileFullPath 
						+ ". Reason: " + e.getMessage());
			}
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			try {
				bw.write(key.toString() + getConfiguration().get(REDUCER_OP_SEPARATOR) + value.toString());
			} catch (IOException e) {
				log.severe("Failed to write record with key as " + key.toString() + " and value as " 
						+ value.toString() + ". Reason: " + e.getMessage());
			}
		}

		public void close() {
			try {
				bw.close();
			} catch (IOException e) {
				log.severe("Failed to close buffered writer for Reason " + e.getMessage());
			}
			
			uploadToS3();
			counter++;
			initializeBufferedWriter();
		}

		private void uploadToS3() {
			File fileToUpload = new File(fileFullPath);
			String s3FullPath = getConfiguration().get(OUTPUT_PATH) + S3_PATH_SEP + fileToUpload.getName();
			log.info("Upload reducer output from " + fileFullPath + " to " + s3FullPath) ;
			s3wrapper.uploadFileS3(s3FullPath, fileToUpload);
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	};

	protected void cleanup(Context context) throws IOException, InterruptedException {}
}
