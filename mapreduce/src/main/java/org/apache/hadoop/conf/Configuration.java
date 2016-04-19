package org.apache.hadoop.conf;

import static org.apache.hadoop.Constants.JobConf.DEFAULT_REDUCER_OP_SEPARATOR;
import static org.apache.hadoop.Constants.JobConf.REDUCER_OP_SEPARATOR;

import java.util.HashMap;
import java.util.Map;

public class Configuration {

	private Map<String, String> properties;

	public Configuration() {
		properties = new HashMap<String, String>(10);
		this.set(REDUCER_OP_SEPARATOR, DEFAULT_REDUCER_OP_SEPARATOR);
	}

	public void set(String key, String value) {
		properties.put(key, value);
	}

	public Map<String, String> getMap() {
		return properties;
	}

	public String get(String key) {
		return properties.get(key);
	}
}
