package org.apache.hadoop.mapreduce.lib.input;

import static org.apache.hadoop.Constants.JobConf.MULTIPLE_INPUT;
import static org.apache.hadoop.Constants.JobConf.MULTIPLE_INPUT_INTERNAL_SEP;
import static org.apache.hadoop.Constants.JobConf.MULTIPLE_INPUT_SEP;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class MultipleInputs {

	public static void addInputPath(Job job, Path inputPath, Class<?> inputFormatClass, Class<?> mapperClass) {
		String inputVal = inputPath.get() + MULTIPLE_INPUT_INTERNAL_SEP + mapperClass.getName() + MULTIPLE_INPUT_SEP;
		String value = job.getConfiguration().get(MULTIPLE_INPUT);
		if (value == null) {
			value = inputVal;	
		}
		else {
			value = value + inputVal;
		}
		job.set(MULTIPLE_INPUT, value);
	}
}
