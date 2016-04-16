package org.apache.hadoop.fs;

public class Path {
	
	String path;
	
	public Path(String path) {
		this.path = path;
	}
	
	public String get() {
		return this.path;
	}
}
