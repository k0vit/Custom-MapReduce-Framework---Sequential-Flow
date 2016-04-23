package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import neu.edu.utilities.Utilities;

/**
 * Base context file shared between mapper and reducer
 * 
 * @author kovit
 *
 */
public class BaseContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> implements IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	
	private static final Logger log = Logger.getLogger(BaseContext.class.getName());
	
	private Map<String, Map<String, IntWritable>> counter = new HashMap<>();
	private Configuration config;
	
	@Override
	public void write(KEYOUT key, VALUEOUT value) {
		log.info("Base Context " + key + " " + value);
	}
	
	/**
	 * Supports counter as in hadoop
	 * 
	 * @param group
	 * @param counterName
	 * @return
	 */
	public IntWritable getCounter(String group, String counterName) {
		if (counter.containsKey(group)) {
			if (counter.get(group).containsKey(counterName)) {
				return counter.get(group).get(counterName);
			}
			else {
				IntWritable i = new IntWritable(0);
				counter.get(group).put(counterName, i);
				return i;
			}
		}
		else {
			counter.put(group, new HashMap<>(5));
			IntWritable i = new IntWritable(0);
			counter.get(group).put(counterName, i);
			return i;
		}
	}
	
	/**
	 * user defined configuration and job configuration
	 * 
	 * @return
	 */
	public Configuration getConfiguration() {
		if (config == null) {
			config = new Configuration();
			Properties jobConfig = Utilities.readPropertyFile(JOB_CONF_PROP_FILE_NAME);	
			for (final String name: jobConfig.stringPropertyNames())
			    config.set(name, jobConfig.getProperty(name));
		}
		
		return this.config;
	}
}
