package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.EOR_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.KEY_URL;
import static org.apache.hadoop.Constants.CommProperties.OK;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.CommProperties.SUCCESS;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.TASK_SPLITTER;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static org.apache.hadoop.Constants.JobConf.MAPPER_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.REDUCER_CLASS;
import static org.apache.hadoop.Constants.MapReduce.MAP_METHD_NAME;
import static org.apache.hadoop.Constants.MapReduce.REDUCE_METHD_NAME;
import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mashape.unirest.http.Unirest;

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
 * stop the server
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
 * ---- upload the contents on s3 - done in context class
 * ---- delete the file
 * 
 * 5) 
 * call /EOM as mapper is done.
 * 
 * 6) 
 * listen to /Key for the set of keys from the master
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

	public static void main(String[] args) {
		/**
		 * step 1
		 */
		post(START_JOB_URL, (request, response) -> {
			log.info("Recieved start signal from the master to start a new job " + request.body());
			response.status(OK);
			response.body(SUCCESS);
			(new Thread(new SlaveJob())).start();
			log.info("Started new thread to handle the job " + request.body());
			return response.body().toString();
		});
	}
}

class SlaveJob implements Runnable {

	private static final Logger log = Logger.getLogger(SlaveJob.class.getName());

	private Properties clusterProperties;
	private S3Wrapper s3wrapper;
	private Properties jobConfiguration;
	private String masterIp;
	private static String filesToProcess;
	private static String keysToProcess;

	@Override
	public void run() {
		map();
		reduce();
		tearDown();
	}

	private void map() { 
		log.info("Starting map task");
		readFiles();
		setup();
		processFiles();
		log.info("All files processed, signalling end of mapper phase");
		NodeCommWrapper.sendData(masterIp, EOM_URL);
	}

	private void readFiles() {
		log.info("Listening on " + FILE_URL + " for files from master");
		post(FILE_URL, (request, response) -> {
			masterIp = request.ip();
			filesToProcess = request.body();
			log.info("Recieved request on " + FILE_URL + " from " + masterIp);
			response.status(OK);
			response.body(SUCCESS);
			return response.body().toString();
		});		

		while (filesToProcess == null) {
			log.info("Waiting for files from master");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.severe("Sleep interrupted while waiting for files from master");
			}
		}

		log.info("Files to process by mapper " + filesToProcess);
		//stop();
	}
	
	/**
	 * step 2
	 */
	private void setup() {
		
		clusterProperties = Utilities.readClusterProperties();
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty(ACCESS_KEY), clusterProperties.getProperty(SECRET_KEY))));
		jobConfiguration = downloadAndReadJobConfig();
		log.info("Slave setup done");
	}

	private Properties downloadAndReadJobConfig() {
		log.info("download and load job configuraiton");
		String s3FilePath = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + JOB_CONF_PROP_FILE_NAME;
		String localFilePath = s3wrapper.readOutputFromS3(s3FilePath, JOB_CONF_PROP_FILE_NAME);
		return Utilities.readPropertyFile(localFilePath);
	}


	/**
	 * Step 4
	 * 
	 * Create a folder called OutputOfMap 
	 * for each file 
	 * -- download the file
	 * -- Instantiate the mapper class 
	 * -- for each record in the file 
	 * ---- call the map method 
	 * -- once the file is done: 
	 * ---- call the close on Context to close all the FileWriter (check Context.write on Mapper below)
	 * ---- upload the contents on s3 (Mapper Context does that as it has the file name)0
	 * ---- delete the file
	 */
	@SuppressWarnings("rawtypes")
	private void processFiles() {
		Utilities.createDirs(IP_OF_MAP, OP_OF_MAP);
		String files[] = filesToProcess.split(TASK_SPLITTER);
		Mapper<?,?,?,?> mapper = instantiateMapper();
		Context context = mapper.new Context();
		for (String file: files) {
			String localFilePath = downloadFile(file);
			processFile(localFilePath, mapper, context);
			context.close();
			new File(localFilePath).delete();
		}
	}

	private String downloadFile(String file) {
		
		String s3FilePath = jobConfiguration.getProperty(INPUT_PATH) + S3_PATH_SEP + file;
		String localFilePath = IP_OF_MAP + File.separator + file;
		return s3wrapper.readOutputFromS3(s3FilePath, localFilePath);
	}

	@SuppressWarnings("rawtypes")
	private void processFile(String file, Mapper<?, ?, ?, ?> mapper, Context context) {
		log.info("Processing file " + file);
		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			String line = null;
			long counter = 0l;
			while((line = br.readLine()) != null) {
				processLine(line, mapper, context, counter);
				counter++;
			}   
		}
		catch(Exception e) {
			log.severe("Failed to read file " + file + ". Reason: " + e.getMessage());
		}
	}

	private Mapper<?, ?, ?, ?> instantiateMapper() {
		log.info("Instanting Mapper");
		Mapper<?,?,?,?> mapper = null;
		try {
			Class<?> cls = getMapreduceClass(jobConfiguration.getProperty(MAPPER_CLASS));
			Constructor<?> ctor = cls.getDeclaredConstructor();
			ctor.setAccessible(true);
			mapper = (Mapper<?, ?, ?, ?>) ctor.newInstance();
			if (mapper != null) {
				log.info("Mapper instantiated successfully " + jobConfiguration.getProperty(MAPPER_CLASS));
			}
		} catch (Exception e) {
			log.severe("Failed to create an instance of mapper class. Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
		return mapper;
	}

	@SuppressWarnings("rawtypes")
	private void processLine(String line, Mapper<?, ?, ?, ?> mapper, Context context, long counter) {
		try {
			Class<?> KEYIN = Class.forName(LongWritable.class.getName());
			Object keyIn = KEYIN.getConstructor(Long.class).newInstance(counter);
			Class<?> VALUEIN = Class.forName(Text.class.getName());
			Object valueIn = VALUEIN.getConstructor(String.class).newInstance(line);
			java.lang.reflect.Method mthd = getMapreduceClass(jobConfiguration.getProperty(MAPPER_CLASS))
					.getMethod(MAP_METHD_NAME, KEYIN, VALUEIN, Mapper.Context.class);
			mthd.setAccessible(true);
			mthd.invoke(mapper, keyIn, valueIn, mapper.new Context());
		} catch (Exception e) {
			log.severe("Failed to invoke map method on mapper class " + mapper 
					+ ". Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
	}

	private void reduce() {
		log.info("Starting reduce task");
		readKeys();
		processKeys();
		log.info("All keys processed. Signalling end of reducer phase");
		NodeCommWrapper.sendData(masterIp, EOR_URL);
	}

	private void readKeys() {
		post(KEY_URL, (request, response) -> {
			masterIp = request.ip();
			keysToProcess = request.body();
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});		
		while (keysToProcess == null) {}
		log.info("Keys to process " + keysToProcess);
		//stop();
	}

	/**
	 * Create a folder Output
	 * For each key 
	 * -- Open file writer to file part-r-00<slaveId>-<file counter>
	 * -- download the key directory from the s3
	 * -- read all the files 
	 * -- generate the iterator
	 * -- Instantiate the reducer class
	 * -- call the reduce method with the iterable
	 * -- Close the context
	 * -- once done delete the key dir
	 * 
	 * */
	private void processKeys() {
		Utilities.createDirs(IP_OF_REDUCE, OP_OF_REDUCE);
		String[] keys = keysToProcess.split(TASK_SPLITTER);
		Reducer<?,?,?,?> reducer = getReducerInstance();
		Reducer<?, ?, ?, ?>.Context context = reducer.new Context();
		for (String key: keys) {
			log.info("Processing key " + key);
			String keyDirPath = downloadKeyFiles(key);
			processKey(keyDirPath, key, reducer, context);
			context.close();
			Utilities.deleteFolder(new File(keyDirPath));
		}
	}

	private String downloadKeyFiles(String key) {
		log.info("Download files for key " + key);
		String keyDir = (key + KEY_DIR_SUFFIX);
		String keyDirLocalPath = IP_OF_REDUCE + File.separator + keyDir;
		String s3KeyDir = BUCKET + S3_PATH_SEP + keyDir;
		s3wrapper.downloadAndStoreFileInLocal(s3KeyDir, System.getProperty("user.dir"));
		log.info("key dir local path " + keyDirLocalPath);
		return keyDirLocalPath;
	}

	private void processKey(String keyDirPath, String key, Reducer<?, ?, ?, ?> reducer,
			Reducer<?, ?, ?, ?>.Context context) {

		Class<?> KEYIN = getReducerInputClass(jobConfiguration.getProperty(MAP_OUTPUT_KEY_CLASS));
		try {
			Method mthdr = getMapreduceClass(jobConfiguration.getProperty(REDUCER_CLASS))
					.getMethod(REDUCE_METHD_NAME, KEYIN, Iterable.class, Reducer.Context.class);
			Object keyInst = KEYIN.getConstructor(String.class).newInstance(key);
			log.info("Invoking reduce method");
			mthdr.setAccessible(true);
			mthdr.invoke(reducer, keyInst, getIterableValue(keyDirPath, key), context);
		}
		catch (Exception e) {
			log.severe("Failed to invoke reduce method on reduce class " + reducer 
					+ ". Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
	}

	private Reducer<?, ?, ?, ?> getReducerInstance() {
		Reducer<?, ?, ?, ?> reducer = null;
		try {
			Class<?> cls = getMapreduceClass(jobConfiguration.getProperty(REDUCER_CLASS));
			Constructor<?> ctor = cls.getDeclaredConstructor();
			ctor.setAccessible(true);
			reducer = (Reducer<?, ?, ?, ?>) ctor.newInstance();
			if (reducer != null) {
				log.info("Reducer class instantiated successfully " + jobConfiguration.getProperty(REDUCER_CLASS));
			}
		} catch (Exception e) {
			log.severe("Failed to create an instance of reducer class " + reducer 
					+ ". Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
		return reducer;
	}

	private Class<?> getMapreduceClass(String className) {
		Class<?> mrClass = null;
		try {
			mrClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			log.severe("Failed to find class " + className 
					+ ". Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
		return mrClass;
	}

	private Class<?> getReducerInputClass(String className) {
		if (className != null) {
			Class<?> c = null;  
			try {
				c = Class.forName(className);
			} catch (ClassNotFoundException e) {
				log.severe("Failed to find class " + className 
						+ ". Reason " + e.getMessage());
				log.severe("Stacktrace " + Utilities.printStackTrace(e));
			}
			return c;
		}
		else {
			return Text.class;
		}
	}

	private List<Object> getIterableValue(String keyDirPath, String key) {
		log.info("Creating iterator for key " + key + " by reading all the file records from " + keyDirPath);
		File[] files  = new File(keyDirPath).listFiles();
		log.info("There are " + files.length + " associated with key " + key);
		List<Object> values = new LinkedList<>();
		Class<?> VALUEIN = getReducerInputClass(jobConfiguration.getProperty(MAP_OUTPUT_VALUE_CLASS));
		for (File file : files) {
			log.info("Fetching all records from file " + file.getAbsolutePath());
			if (file.getName().startsWith(key)) {
				try (BufferedReader br = new BufferedReader(new FileReader(file))){
					String line = null;
					while((line = br.readLine()) != null) {
						values.add(VALUEIN.getConstructor(String.class).newInstance(line));
					}
				}
				catch(Exception e) {
					log.severe("Failed to create iterable for key " + key 
							+ ". Reason " + e.getMessage());
					log.severe("Stacktrace " + Utilities.printStackTrace(e));
				}
			}
		}
		return values;
	}

	private void tearDown() {
		log.info("Shutting off Unirest process");
		try {
			Unirest.shutdown();
		} catch (IOException e) {
			log.severe("Failed to shutdown Unirest process. Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
	}
}