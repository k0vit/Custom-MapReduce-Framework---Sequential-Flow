package org.apache.hadoop.io;

public class FloatWritable {

	private float value;
	
	public FloatWritable(float value) {
		this.value = value;
	}
	
	public FloatWritable(String value) {
		this.value = Float.parseFloat(value);
	}
	
	public float get() {
		return this.value;
	}
}
