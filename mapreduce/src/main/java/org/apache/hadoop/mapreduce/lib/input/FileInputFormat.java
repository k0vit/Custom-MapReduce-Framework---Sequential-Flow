package org.apache.hadoop.mapreduce.lib.input;

import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class FileInputFormat {
	
	public static void addInputPath(Job job, Path inputPath) {
		job.set(INPUT_PATH, inputPath.get());
	}
}
