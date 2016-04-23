package org.apache.hadoop.mapreduce.lib.output;

import static org.apache.hadoop.Constants.JobConf.OUTPUT_PATH;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;;

/**
 * 
 * @author kovit
 *
 */
public class FileOutputFormat {
	
	public static void setOutputPath(Job job, Path path) {
		job.set(OUTPUT_PATH, path.get());
	}
}
