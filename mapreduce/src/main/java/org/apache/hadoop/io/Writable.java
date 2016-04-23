package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Provides Serialization of primitives and custom defined types
 * 
 * @author kovit
 *
 */
public interface Writable extends Serializable{
	void write(DataOutput out) throws IOException;
	void readFields(DataInput in) throws IOException;
}
