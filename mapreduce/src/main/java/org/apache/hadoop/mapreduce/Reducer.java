package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

	public class Context implements IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			// TODO Auto-generated method stub

		}
		
		public IntWritable getCounter(String string, String uniqueCarrier) {
			// TODO 
			return null;
		}

		public Configuration getConfiguration() {
			// TODO
			return null;
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	};

	protected void cleanup(Context context) throws IOException, InterruptedException {}
}
