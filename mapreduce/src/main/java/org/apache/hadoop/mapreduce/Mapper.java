package org.apache.hadoop.mapreduce;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	public class Context implements IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			// TODO Auto-generated method stub

		}
	}

	protected void setup(Context context){};

	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) {
		context.write((KEYOUT) key, (VALUEOUT) value); 
	}

	protected void cleanup(Context context) {}
}
