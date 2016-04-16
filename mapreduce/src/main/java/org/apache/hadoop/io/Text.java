package org.apache.hadoop.io;

public class Text {

	private String value;
	
	public Text(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return value;
	}
	
	public String get() {
		return value;
	}
}
