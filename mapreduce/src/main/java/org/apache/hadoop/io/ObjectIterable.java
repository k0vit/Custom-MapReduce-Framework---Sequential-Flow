package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.logging.Logger;

import neu.edu.utilities.Utilities;

public class ObjectIterable <T extends Writable> implements Iterable<T> , Iterator<T>{

	private static final Logger log = Logger.getLogger(ObjectIterable.class.getName());

	private T data = null;
	private DataInputStream dis;
	private File[] keyFiles;
	private int currentFile = 0;
	private String key;

	public ObjectIterable(String className, File[] files, String key) {
		log.info("Initializing ObjectIterable with class " + className + " and file name as " + files);
		instantiateData(className);
		this.key = key;
		keyFiles = files;
		initalizeDIS();
	}

	@SuppressWarnings("unchecked")
	private void instantiateData(String className) {
		try {
			Class<?> cls = Class.forName(className);
			cls.getDeclaredConstructor();
			Constructor<?> ctor = cls.getDeclaredConstructor();
			ctor.setAccessible(true);
			data = (T)ctor.newInstance();
		} catch (Exception e) {
			log.severe("Failed to instantiate class " + className);
			Utilities.printStackTrace(e);
		}
	}

	private boolean initalizeDIS() {
		try {
			while(!keyFiles[currentFile].getName().startsWith(key)) {
				++currentFile;
				if (currentFile == keyFiles.length) {
					log.info("Done. Number of files processed " + currentFile);
					return false;
				} 
			}
			dis = new DataInputStream(new FileInputStream(keyFiles[currentFile]));
			return true;
		} catch (FileNotFoundException e) {
			log.severe("Failed to instantiate DataInputStream on file " + keyFiles[currentFile]);
			Utilities.printStackTrace(e);
			return false;
		}
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		try {
			data.readFields(dis);
			return true;
		} 
		catch (IOException e1) {
			if (e1 instanceof EOFException) {
				return handleEOF();
			}
			else {
				log.severe("Failed to check if there is more data. Reason " + e1.getMessage());
				Utilities.printStackTrace(e1);
			}
			return false;
		}
	}

	private boolean handleEOF() {
		try {
			dis.close();
		} catch (IOException e) {
			log.severe("Failed to close DataInputStream");
			Utilities.printStackTrace(e);
		}

		++currentFile;
		if (currentFile == keyFiles.length) {
			log.info("EOF after processig file " + currentFile + " out of " + keyFiles.length);
			return false;
		}
		else {
			return initalizeDIS();
		}
	}


	@Override
	public T next() {
		return data;
	}
}
