package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.JobConf.JAR_BY_CLASS;
import static org.apache.hadoop.Constants.JobConf.JOB_NAME;
import static org.apache.hadoop.Constants.JobConf.MAPPER_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAPPER_INPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAPPER_INPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.NUM_REDUCER;
import static org.apache.hadoop.Constants.JobConf.OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.REDUCER_CLASS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import neu.edu.mapreduce.master.Master;

public class Job {

	private Configuration conf;
	
	private Job(Configuration conf, String jobName) {
		this.conf = conf;
		conf.set(JOB_NAME, jobName);
	}
	
	public static Job getInstance(Configuration conf, String jobName) {
		Job job = new Job(conf, jobName);
		conf.set(OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(OUTPUT_VALUE_CLASS, Text.class.getName());
		conf.set(MAP_OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(MAP_OUTPUT_VALUE_CLASS, Text.class.getName());
		conf.set(MAPPER_INPUT_KEY_CLASS, LongWritable.class.getName());
		conf.set(MAPPER_INPUT_VALUE_CLASS, Text.class.getName());
		return job;
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
	
	public Configuration getConfiguration() {
		return conf;
	}
	
	public boolean waitForCompletion(boolean verbose) {
		Master master = new Master(this);
		return master.submit();
	}

	public String getJobName() {
		return conf.get(JOB_NAME);
	}

	public void setNumReduceTasks(int i) {
		conf.set(NUM_REDUCER, String.valueOf(i));
	}
}
