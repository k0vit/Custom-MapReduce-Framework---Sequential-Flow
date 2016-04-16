package org.apache.hadoop.mapreduce;

public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

	public class Context implements IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			// TODO Auto-generated method stub

		}
	}

	protected void setup(Context context){};

	@SuppressWarnings("unchecked")
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	};

	protected void cleanup(Context context) {}
}
