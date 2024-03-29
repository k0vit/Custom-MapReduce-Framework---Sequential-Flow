package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.io.Writable;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;
/**
 * 
 * @author kovit
 * @author Naineel Shah
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private static final Logger log = Logger.getLogger(Mapper.class.getName());

	/**
	 * Context.write of Mapper [context.write(key, value)]
	 * -- for each key 
	 * ---- check if the key exist in the map maintained by the Context class [Map<String, FileWriter>]
	 * ---- if the key is not present:
	 * ------ create a dir called <key>_key_dir and create a file with <key>_timestamp_<slaveid> 
	 * ------ open  DataInputStream for that file and put in the map
	 * ---- get the DataInputStream from the map and write the record to it
	 * 
	 * -- Once the file is processed i.e. on close upload all the files to s3
	 * 
	 * @author kovit
	 *
	 */
	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

		Map<String, DataOutputStream> keyToFile;
		private S3Wrapper s3wrapper;
		private Properties clusterProperties;
		private String slaveId;

		public Context() {
			super();
			keyToFile = new HashMap<>(100);
			clusterProperties = Utilities.readClusterProperties();
			s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
					(clusterProperties.getProperty(ACCESS_KEY), clusterProperties.getProperty(SECRET_KEY))));
			slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
			log.info("Initializing slave mapper task with Slave id " + slaveId);
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			log.fine("context write invoked with key " + key.toString() + " and value as " + value.toString());
			if (!keyToFile.containsKey(key.toString())) {
				String filePath = System.getProperty("user.dir") + File.separator + OP_OF_MAP + File.separator
						+ key + KEY_DIR_SUFFIX  + key + "_" +
						(new SimpleDateFormat("yyyyMMddhhmm").format(new Date())) + slaveId;

				try {
					log.fine("Creating mapper output file " + filePath);
					File f = new File(filePath);
					if (!f.exists()) {
						f.getParentFile().mkdirs();
						f.createNewFile();
					}
					keyToFile.put(key.toString(), new DataOutputStream(new FileOutputStream(filePath)));
				} catch (IOException e) {
					log.severe("Failed to create file " + filePath + ". Reason " + e.getMessage());
				}
			}

			try {
				((Writable)value).write(keyToFile.get(key.toString()));
			} catch (IOException e) {
				log.severe("Failed to write " + value.toString() + " for key " + key.toString()
				+ "Reason " + e.getMessage());
			}
		}

		public void close() {
			closeAllDataInputStream();
			uploadToS3();
			keyToFile.clear();
		}

		private void closeAllDataInputStream() {
			log.fine("Closing all the BufferedWriter " + keyToFile.size());
			for(String key: keyToFile.keySet()) {
				DataOutputStream dos = keyToFile.get(key);
				try {
					dos.close();
				} catch (IOException e) {
					log.severe("Failed to close buffered writer for key " + key + ". Reason " + e.getMessage());
				}
			}
		}

		private void uploadToS3() {
			for(String key: keyToFile.keySet()) {
				log.fine("uploading mapper output file with respect to key " + key);
				String keyDir = (key + KEY_DIR_SUFFIX);
				String prefix = IP_OF_REDUCE + File.separator + keyDir;
				String bucket = clusterProperties.getProperty(BUCKET);
				String keyLocalDir = OP_OF_MAP + File.separator + keyDir;
				File dir = new File(keyLocalDir);
				if (dir.exists() && dir.isDirectory()) {
					File[] files = dir.listFiles();
					if (files != null) {
						for (File file: dir.listFiles()) {
							if (file.getName().startsWith(key)) {
								String s3FullPath = bucket + S3_PATH_SEP + prefix + file.getName();
								s3wrapper.uploadFileS3(s3FullPath, file);
							}
						}
					}
				}
			}
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value); 
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {}
}
