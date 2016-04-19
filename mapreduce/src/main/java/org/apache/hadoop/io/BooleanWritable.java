package org.apache.hadoop.io;

public class BooleanWritable {

	private boolean value;
	
	public BooleanWritable(Boolean value) {
		this.value = value;
	}
	
	public BooleanWritable(String value) {
		this.value = Boolean.parseBoolean(value);
	}
	
	public boolean get() {
		return this.value;
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
}
