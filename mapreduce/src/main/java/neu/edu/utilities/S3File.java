package neu.edu.utilities;

/**
 * Represents an object in  s3
 * 
 * @author kovit
 *
 */
public class S3File implements Comparable<S3File>{
	
	private String file;
	private Long size;
	
	public String getFileName() {
		return file;
	}
	
	public long getSize() {
		return size;
	}
	
	public S3File(String file, long size) {
		super();
		this.file = file;
		this.size = size;
	}
	
	@Override
	public String toString() {
		return "S3File [file=" + file + ", size=" + size + "]";
	}

	@Override
	public int compareTo(S3File o) {
		return this.size.compareTo(o.getSize());
	}
}
