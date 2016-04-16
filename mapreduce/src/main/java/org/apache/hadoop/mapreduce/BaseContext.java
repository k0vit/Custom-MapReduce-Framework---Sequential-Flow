package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

public class BaseContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> implements IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	
	private Map<String, Map<String, IntWritable>> counter = new HashMap<>();

	@Override
	public void write(KEYOUT key, VALUEOUT value) {
		// TODO
	}
	
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
	
	public Configuration getConfiguration() {
		// basically reading the conf file from s3 and creating properties out of it
		return null;
	}
}
