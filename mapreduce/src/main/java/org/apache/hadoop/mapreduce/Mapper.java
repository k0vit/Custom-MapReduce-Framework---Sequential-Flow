package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			System.out.println(key + " " + value);
			// TODO
		}
		
		public void close() {
			// TODO close filewriter
			// TODO upload files to s3
		}
		
		private void uploadToS3() {}
	}

	public void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value); 
	}

	public void cleanup(Context context) throws IOException, InterruptedException {}
}
