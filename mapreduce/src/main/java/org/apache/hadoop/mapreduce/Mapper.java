package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private static final Logger log = Logger.getLogger(Mapper.class.getName());

	/**
	 * Context.write of Mapper [context.write(key, value)]
	 * -- for each key 
	 * ---- check if the key exist in the map maintained by the Context class [Map<String, FileWriter>]
	 * ---- if the key is not present:
	 * ------ create a dir called <key>_key_dir and create a file with <key>_timestamp_<slaveid> 
	 * ------ open  FileWriter for that file and put in the map
	 * ---- get the FileWriter from the map and write the record to it
	 * 
	 * @author kovit
	 *
	 */
	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

		//Map<String, BufferedWriter> keyToFile;
		private S3Wrapper s3wrapper;
		private Properties clusterProperties;
		private String slaveId;
		Map<String, LinkedList<String>> keyValueStore;

		public Context() {
			super();
			//keyToFile = new HashMap<>();
			clusterProperties = Utilities.readClusterProperties();
			s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
					(clusterProperties.getProperty(ACCESS_KEY), clusterProperties.getProperty(SECRET_KEY))));
			slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
			log.info("Initializing slave mapper task with Slave id " + slaveId);
			keyValueStore = new HashMap<>();
		}

		/*@Override
		public void write(KEYOUT key, VALUEOUT value) {
			slog.fine("context write invoked with key " + key.toString() + " and value as " + value.toString());
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
					keyToFile.put(key.toString(), new BufferedWriter(new FileWriter(filePath, true)));
				} catch (Exception e) {
					log.severe("Failed to create file " + filePath + ". Reason " + e.getMessage());

				}
			}

			try {
				keyToFile.get(key.toString()).write(value.toString() + System.getProperty("line.separator"));
			} catch (IOException e) {
				log.severe("Failed to write " + value.toString() + " for key " + key.toString()
				+ "Reason " + e.getMessage());
			}
		}*/

		/*public void close() {
			closeAllFileWriter();
			uploadToS3();
			keyToFile.clear();
		}*/

		/*private void closeAllFileWriter() {
			log.fine("Closing all the BufferedWriter " + keyToFile.size());
			for(String key: keyToFile.keySet()) {
				BufferedWriter bw = keyToFile.get(key);
				try {
					bw.close();
				} catch (IOException e) {
					log.severe("Failed to close buffered writer for key " + key + ". Reason " + e.getMessage());
				}
			}
		}*/

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			String keyStr = key.toString();
			String valueStr = value.toString();
			log.fine("context write invoked with key " + keyStr + " and value as " + valueStr);
			if (!keyValueStore.containsKey(keyStr)) {
				keyValueStore.put(keyStr, new LinkedList<>());
			}
			keyValueStore.get(keyStr).add(valueStr);
		}

		public void close() {
			log.info("Closing all the keys related to this file");
			writeToFile();
			uploadDirToS3();
			keyValueStore = new HashMap<>();
			//s3wrapper.waitTillUploadCompletes();
			//Utilities.deleteFolder(new File(OP_OF_MAP));
		}	

		private void uploadDirToS3() {
			log.fine("uploading mapper output directory");
			String bucket = clusterProperties.getProperty(BUCKET);
			s3wrapper.uploadFilesToS3(bucket, new File(OP_OF_MAP));
		}

		private void writeToFile() {
			String filePath = null;
			log.info("Closing " + keyValueStore.size() + " keys");
			for (String key: keyValueStore.keySet()) {
				String keyStr = key.toString();
				filePath = System.getProperty("user.dir") + File.separator + OP_OF_MAP + File.separator
						+ keyStr + KEY_DIR_SUFFIX  + keyStr + "_" +
						(new SimpleDateFormat("yyyyMMddhhmm").format(new Date())) + slaveId;

				try {
					log.fine("Creating mapper output file " + filePath);
					File f = new File(filePath);
					if (!f.exists()) {
						f.getParentFile().mkdirs();
						f.createNewFile();
					}
				} catch (Exception e) {
					log.severe("Failed to create file " + filePath + ". Reason " + e.getMessage());

				}

				try(BufferedWriter bw = new BufferedWriter(new FileWriter(filePath, true))) {
					for (String value: keyValueStore.get(keyStr)) {
						bw.write(value + System.getProperty("line.separator"));
					}
				}
				catch (Exception e) {
					log.severe("Failed to write for key " + keyStr + "Reason " + e.getMessage());
				}
				
				//uploadToS3(keyStr);
			}
		}

		private void uploadToS3(String key) {
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
							s3wrapper.uploadFileS3(s3FullPath, file, false);
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
