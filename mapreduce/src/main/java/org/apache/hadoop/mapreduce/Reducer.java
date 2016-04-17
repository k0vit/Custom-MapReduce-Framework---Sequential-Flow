package org.apache.hadoop.mapreduce;

import java.io.IOException;

public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			System.out.println(key + " " + value);
		}
	}

	public void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	};

	public void cleanup(Context context) throws IOException, InterruptedException {}
}
