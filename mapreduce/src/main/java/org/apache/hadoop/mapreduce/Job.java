package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.JobConstants.JAR_BY_CLASS;
import static org.apache.hadoop.Constants.JobConstants.JOB_NAME;
import static org.apache.hadoop.Constants.JobConstants.MAPPER_CLASS;
import static org.apache.hadoop.Constants.JobConstants.MAP_OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConstants.MAP_OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConstants.OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConstants.OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConstants.REDUCER_CLASS;

import org.apache.hadoop.conf.Configuration;

public class Job {

	private Configuration conf;
	
	private Job(Configuration conf, String jobName) {
		this.conf = conf;
		conf.set(JOB_NAME, jobName);
	}
	
	public static Job getInstance(Configuration conf, String jobName) {
		return new Job(conf, jobName); 
	}
	
	public void setMapperClass(Class<?> name) {
		conf.set(MAPPER_CLASS, name.getName());
	}
	
	public void setReducerClass(Class<?> name) {
		conf.set(REDUCER_CLASS, name.getName());
	}
	
	public void setOutputKeyClass(Class<?> name) {
		conf.set(OUTPUT_KEY_CLASS, name.getName());
	}
	
	public void setOutputValueClass(Class<?> name) {
		conf.set(OUTPUT_VALUE_CLASS, name.getName());
	}
	
	public void setMapOutputKeyClass(Class<?> name) {
		conf.set(MAP_OUTPUT_KEY_CLASS, name.getName());
	}
	
	public void setMapOutputValueClass(Class<?> name) {
		conf.set(MAP_OUTPUT_VALUE_CLASS, name.getName());
	}
	
	public void set(String key, String value) {
		conf.set(key, value);
	}
	
	public void setJarByClass(Class<?> name) {
		conf.set(JAR_BY_CLASS, name.getName());
	}
	
	public boolean waitForCompletion(boolean verbose) {
		// TODO
		return false;
	}
}
