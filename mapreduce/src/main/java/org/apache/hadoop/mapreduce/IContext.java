package org.apache.hadoop.mapreduce;

public interface IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	
	void write(KEYOUT key, VALUEOUT value);
}
