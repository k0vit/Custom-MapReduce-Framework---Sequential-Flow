package org.apache.hadoop.io;

public class LongWritable {

	private long value;
	
	public LongWritable(long value) {
		this.value = value;
	}
	
	public LongWritable(String value) {
		this.value = Long.parseLong(value);
	}
	
	public long get() {
		return this.value;
	}
}
