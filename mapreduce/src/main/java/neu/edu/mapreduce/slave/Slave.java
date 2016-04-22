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
import static org.apache.hadoop.Constants.JobConf.JOB_NAME;
import static org.apache.hadoop.Constants.JobConf.MAPPER_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.REDUCER_CLASS;
import static org.apache.hadoop.Constants.MapReduce.MAP_METHD_NAME;
import static org.apache.hadoop.Constants.MapReduce.NOKEY;
import static org.apache.hadoop.Constants.MapReduce.REDUCE_METHD_NAME;
import static org.apache.hadoop.Constants.MapReduce.START_MAPPER;
import static spark.Spark.post;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectIterable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

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
	private static String masterIp;
	private static List<MapTask> maptasks = Collections.synchronizedList(new ArrayList<>(3));
	private static String keysToProcess;
	private int totalTasksFileCount;
	private int currentTasksFileCount;
	private static boolean startMap = false;
	private static int totalMapTasksCount = 0;
	private static AtomicInteger currentMapTasksCount = new AtomicInteger(0);

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
		cleanup(true);

		for(MapTask task: maptasks) {
			log.info("Handlig map task " + task.toString());
			processFiles(task);
			totalTasksFileCount = 0;
			currentTasksFileCount = 0;
		}

		log.info("All files processed, signalling end of mapper phase");
		if (masterIp == null) {
			masterIp = Utilities.getMasterIp(Utilities.readInstanceDetails());
		}
		NodeCommWrapper.sendData(masterIp, EOM_URL);
	}

	private void readFiles() {
		log.info("Listening on " + FILE_URL + " for files from master");
		post(FILE_URL, (request, response) -> {
			log.info("Current thread " + Thread.currentThread().getName());
			masterIp = request.ip();
			String reqBody = request.body();
			if (reqBody.startsWith(START_MAPPER)) {
				log.info("Received start mapper signal " + reqBody);
				startMap = true;
				totalMapTasksCount = Integer.valueOf(reqBody.replace(START_MAPPER, ""));
				log.info("total number of mappers " + totalMapTasksCount);
			}
			else {
				currentMapTasksCount.incrementAndGet();
				MapTask task = new MapTask(reqBody); 
				maptasks.add(task);
				log.info("Received map tasks " + task.toString());
				log.info("Current Map task received " + currentMapTasksCount.get());
			}
			log.info("Recieved request on " + FILE_URL + " from " + masterIp);
			response.status(OK);
			response.body(SUCCESS);
			return response.body().toString();
		});		

		while (! (startMap && currentMapTasksCount.get() == totalMapTasksCount)) {
			log.info("Waiting for files from master");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.severe("Sleep interrupted while waiting for files from master");
			}
		}

		log.info("All Map tasks received " + currentTasksFileCount);
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
		Thread.currentThread().setName(jobConfiguration.getProperty(JOB_NAME));
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
	private void processFiles(MapTask task) {
		Utilities.createDirs(IP_OF_MAP, OP_OF_MAP);
		Mapper<?,?,?,?> mapper = instantiateMapper(task.mapperClassName);
		Context context = mapper.new Context();
		totalTasksFileCount = task.filesToProcess.size();
		for (String file: task.filesToProcess) {
			log.info("Processing file " + ++currentTasksFileCount + " out of " + totalTasksFileCount);
			String localFilePath = downloadFile(file, task.inputPath);
			processFile(localFilePath, mapper, context);
			context.close();
			new File(localFilePath).delete();
		}
	}

	private String downloadFile(String file, String inputPath) {

		String s3FilePath = inputPath + S3_PATH_SEP + file;
		String localFilePath = IP_OF_MAP + File.separator + file;
		return s3wrapper.readOutputFromS3(s3FilePath, localFilePath);
	}

	@SuppressWarnings("rawtypes")
	private void processFile(String file, Mapper<?, ?, ?, ?> mapper, Context context) {
		log.info("Processing file " + file);
		try (BufferedReader br = new BufferedReader(new InputStreamReader
				(new GZIPInputStream(new FileInputStream(file))))){
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

	private Mapper<?, ?, ?, ?> instantiateMapper(String mapperClassName) {
		log.fine("Instanting Mapper");
		Mapper<?,?,?,?> mapper = null;
		try {
			Class<?> cls = getMapreduceClass(mapperClassName);
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
			log.fine("Calling map with key as " + counter + " value as " + line);
			Class<?> KEYIN = Class.forName(LongWritable.class.getName());
			Object keyIn = KEYIN.getConstructor(Long.class).newInstance(counter);
			Class<?> VALUEIN = Class.forName(Text.class.getName());
			Object valueIn = VALUEIN.getConstructor(String.class).newInstance(line);
			java.lang.reflect.Method mthd = getMapreduceClass(jobConfiguration.getProperty(MAPPER_CLASS))
					.getDeclaredMethod(MAP_METHD_NAME, KEYIN, VALUEIN, Mapper.Context.class);
			mthd.setAccessible(true);
			mthd.invoke(mapper, keyIn, valueIn, context);
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
		if (masterIp == null) {
			masterIp = Utilities.getMasterIp(Utilities.readInstanceDetails());
		}
		NodeCommWrapper.sendData(masterIp, EOR_URL);
	}

	private void readKeys() {
		log.info("Listening on " + KEY_URL);
		post(KEY_URL, (request, response) -> {
			log.info("Current thread " + Thread.currentThread().getName());
			masterIp = request.ip();
			keysToProcess = request.body();
			log.info("Recieved request from " + masterIp + " with data as " + keysToProcess);
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});	

		while (keysToProcess == null) {
			log.info("Waiting for keys from master");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.severe("Sleep interrupted while waiting for keys from master");
			}
		}

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
		totalTasksFileCount = keys.length;
		for (String key: keys) {
			log.info("Processing file " + ++currentTasksFileCount + " out of " + totalTasksFileCount);
			if (!key.equals(NOKEY)) {
				log.fine("Processing key " + key);
				String keyDirPath = downloadKeyFiles(key);
				processKey(keyDirPath, key, reducer, context);
				Utilities.deleteFolder(new File(keyDirPath));
			}
		}
		context.close();
	}

	private String downloadKeyFiles(String key) {
		log.fine("Download files for key " + key);
		String keyDir = (key + KEY_DIR_SUFFIX);
		String keyDirLocalPath = IP_OF_REDUCE + File.separator + keyDir;
		String s3KeyDir = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + IP_OF_REDUCE + S3_PATH_SEP + keyDir;
		s3KeyDir = s3KeyDir.substring(0, s3KeyDir.lastIndexOf(S3_PATH_SEP));
		s3wrapper.downloadDir(s3KeyDir, System.getProperty("user.dir"));
		log.fine("key dir local path " + keyDirLocalPath);
		return keyDirLocalPath;
	}

	private void processKey(String keyDirPath, String key, Reducer<?, ?, ?, ?> reducer,
			Reducer<?, ?, ?, ?>.Context context) {

		Class<?> KEYIN = getReducerInputClass(jobConfiguration.getProperty(MAP_OUTPUT_KEY_CLASS));
		try {
			Method mthdr = getMapreduceClass(jobConfiguration.getProperty(REDUCER_CLASS))
					.getDeclaredMethod(REDUCE_METHD_NAME, KEYIN, Iterable.class, Reducer.Context.class);
			Object keyInst = KEYIN.getConstructor(String.class).newInstance(key);
			log.fine("Invoking reduce method");
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

	@SuppressWarnings("rawtypes")
	private ObjectIterable getIterableValue(String keyDirPath, String key) {
		log.info("Creating iterator for key " + key + " by reading all the file records from " + keyDirPath);
		File[] files  = new File(System.getProperty("user.dir") + File.separator + keyDirPath).listFiles();
		log.info("There are " + files.length + " associated with key " + key);
		return new ObjectIterable(jobConfiguration.getProperty(MAP_OUTPUT_VALUE_CLASS), files, key);
	}

	private void tearDown() {
		log.info("Cleaning directories and shutting Transfermanager");
		cleanup(false);
		s3wrapper.shutDown();
		maptasks.clear();
		keysToProcess = null;
	}

	private void cleanup(boolean isStartup) {
		Utilities.deleteFolder(new File(IP_OF_MAP));
		Utilities.deleteFolder(new File(OP_OF_MAP));
		Utilities.deleteFolder(new File(IP_OF_REDUCE));
		Utilities.deleteFolder(new File(OP_OF_REDUCE));
	}
}

class MapTask {
	List<String> filesToProcess;
	String mapperClassName;
	String inputPath;
	public MapTask(String taskData) {
		String[] data = taskData.split(TASK_SPLITTER);
		filesToProcess = new ArrayList<>(data.length);
		for (int i=0;i<data.length;i++) {
			if (i==data.length-1) {
				mapperClassName = data[i];
			}
			else if (i==data.length-2) {
				inputPath = data[i];
			}
			else {
				filesToProcess.add(data[i]);
			}
		}
	}
	@Override
	public String toString() {
		return "MapTask [filesToProcess=" + filesToProcess
				+ ", mapperClassName=" + mapperClassName + ", inputPath="
				+ inputPath + "]";
	}
}