package org.apache.hadoop.conf;

import static org.apache.hadoop.Constants.JobConf.DEFAULT_REDUCER_OP_SEPARATOR;
import static org.apache.hadoop.Constants.JobConf.REDUCER_OP_SEPARATOR;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Configuration {

	private static final Logger log = Logger.getLogger(Configuration.class.getName());
	private Map<String, String> properties;

	public Configuration() {
		properties = new HashMap<String, String>(10);
		this.set(REDUCER_OP_SEPARATOR, DEFAULT_REDUCER_OP_SEPARATOR);
	}

	public void set(String key, String value) {
		properties.put(key, value);
		log.info("Setting job config property with key " + key + " and value as " + value);
	}

	public Map<String, String> getMap() {
		return properties;
	}

	public String get(String key) {
		String value = properties.get(key); 
		log.fine("Property fetched with key " + key + "and value as " + value);
		return value;
	}
}
