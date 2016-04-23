package org.apache.hadoop.mapreduce;

/**
 * 
 * @author kovit
 *
 */
public interface IContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	
	void write(KEYOUT key, VALUEOUT value);
}
