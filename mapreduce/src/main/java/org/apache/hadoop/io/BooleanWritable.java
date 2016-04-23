package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents boolean primitive type
 * 
 * @author kovit
 *
 */
@SuppressWarnings("serial")
public class BooleanWritable implements Writable {

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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readBoolean();
	}
}
