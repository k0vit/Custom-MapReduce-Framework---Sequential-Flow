package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents long primitive type
 * @author kovit
 *
 */
@SuppressWarnings("serial")
public class LongWritable implements Writable{

	private long value;
	
	public LongWritable(Long value) {
		this.value = value;
	}
	
	public LongWritable(String value) {
		this.value = Long.parseLong(value);
	}
	
	public long get() {
		return this.value;
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(value);;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readLong();
	}
}
