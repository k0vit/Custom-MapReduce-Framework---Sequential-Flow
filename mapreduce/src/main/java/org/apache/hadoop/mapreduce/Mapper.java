package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		
		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			// TODO Auto-generated method stub
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value); 
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {}
}
