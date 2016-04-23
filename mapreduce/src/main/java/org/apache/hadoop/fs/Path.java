package org.apache.hadoop.fs;

/**
 * Representing s3 path
 * 
 * @author kovit
 *
 */
public class Path {
	
	String path;
	
	public Path(String path) {
		this.path = path;
	}
	
	public String get() {
		return this.path;
	}
}
