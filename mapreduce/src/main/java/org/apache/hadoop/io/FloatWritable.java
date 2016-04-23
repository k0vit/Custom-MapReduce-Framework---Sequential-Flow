package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents float primitive type
 * @author kovit
 *
 */
@SuppressWarnings("serial")
public class FloatWritable implements Writable{

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
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readFloat();
	}
}
