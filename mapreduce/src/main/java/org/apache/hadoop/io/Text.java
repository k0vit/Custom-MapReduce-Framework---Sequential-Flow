package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Text implements Writable{

	private String value;
	
	public Text(String value) {
		this.value = value;
	}
	
	public Text(Text key) {
		this.value = key.get();
	}

	@Override
	public String toString() {
		return value;
	}
	
	public String get() {
		return value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(value);;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readUTF();
	}
}
