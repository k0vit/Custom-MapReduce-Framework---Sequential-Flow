package org.apache.hadoop.io;

public class DoubleWritable {

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
}
