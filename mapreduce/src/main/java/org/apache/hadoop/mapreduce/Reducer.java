package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.PART_FILE_PREFIX;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.JobConf.REDUCER_OP_SEPARATOR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

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

			initializeBufferedWriter();
		}

		private void initializeBufferedWriter() {
			fileFullPath = OP_OF_REDUCE + PART_FILE_PREFIX + slaveId + counter;
			try {
				bw = new BufferedWriter(new FileWriter(fileFullPath));
			} catch (IOException e) {
				// TODO
			}
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			try {
				bw.write(key.toString() + getConfiguration().get(REDUCER_OP_SEPARATOR) + value.toString());
			} catch (IOException e) {
				// TODO
			}
		}

		public void close() {
			try {
				bw.close();
			} catch (IOException e) {
				// TODO
			}
			uploadToS3();
			counter++;
			initializeBufferedWriter();
		}

		private void uploadToS3() {
			File fileToUpload = new File(fileFullPath);
			String s3FullPath = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + fileToUpload.getName();
			s3wrapper.uploadFileS3(s3FullPath, fileToUpload);
		}
	}

	public void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	};

	public void cleanup(Context context) throws IOException, InterruptedException {}
}
