package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleWritable implements Writable{

	private double value;

	public DoubleWritable(double value) {
		this.value = value;
	}

	public DoubleWritable(String value) {
		this.value = Double.parseDouble(value);
	}

	public double get() {
		return this.value;
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readDouble();
	}
}
