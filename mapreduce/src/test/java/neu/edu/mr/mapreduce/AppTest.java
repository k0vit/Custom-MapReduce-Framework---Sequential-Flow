package neu.edu.mr.mapreduce;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import neu.edu.utilities.S3Wrapper;

/**
 * Unit test for simple App.
 */
public class AppTest 
extends TestCase
{
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public AppTest( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( AppTest.class );
	}
	
	public void testListObjects() {
		S3Wrapper s = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				("AKIAJF77SYNJS5ZLR2TA", "t+cWGKYOOplu1ib53CIFf7XgH56brRsZd3HKsl00")));

		System.out.println(s.getListOfObjects("s3://kovit3/outputa7/OutputOfJob1"));
	}

	/*public void testFileDistr() {
		List<Node> nodes = new ArrayList<>();
		Node n = new Node("", "", "S", "1");
		Node n2 = new Node("", "", "S", "2");
		Node n3 = new Node("", "", "S", "3");
		nodes.add(n);
		nodes.add(n2);
		nodes.add(n3);
		S3Wrapper s = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				("AKIAIKLFBEJTEAZR76TA", "KJN0ypHB6Ct/hVccOKupv0kFiOoYZYSLVQfNlyU8")));

		List<S3File> s3Files = s.getListOfObjects("s3://kovit/input-half");
		Collections.sort(s3Files);
		Collections.reverse(s3Files);

		List<NodeToTask> nodeToFile = new ArrayList<>(2); 
		for (Node node : nodes) {
			nodeToFile.add(new NodeToTask(node));
		}

		for (S3File file : s3Files) {
			if (file.getFileName().endsWith(GZ_FILE_EXT)) {
				nodeToFile.get(0).addToTaskLst(file.getFileName(), true);
				nodeToFile.get(0).addToTotalSize(file.getSize());
			}
			Collections.sort(nodeToFile);
		}

		for (NodeToTask task : nodeToFile) {
			System.out.println(task);
		}
	}

	public void testListObjects() {
		S3Wrapper s = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				("AKIAJNNJKLOMJRHGRMOA", "gvfoLO/iAo88Et6iPY3FqqercaF2P7wqRIeMcezh")));

		List<S3File> file = s.getListOfObjects("kovit", "test/2_key_dir");
		System.out.println(file);
	}

	public void testKeys() {
		List<Node> nodes = new ArrayList<>();
		Node n = new Node("", "", "S", "1");
		Node n2 = new Node("", "", "S", "2");
		Node n3 = new Node("", "", "S", "3");
		nodes.add(n);
		nodes.add(n2);
		nodes.add(n3);
		S3Wrapper s = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				("AKIAIKLFBEJTEAZR76TA", "KJN0ypHB6Ct/hVccOKupv0kFiOoYZYSLVQfNlyU8")));
		List<S3File> s3Files = s.getListOfObjects("s3://kovit/output/test");

		Map<String, Long> keyToSize = new HashMap<>();
		String key = null;
		for (S3File file : s3Files) {
			String fileName = file.getFileName();
			if (file.getFileName().endsWith(KEY_DIR_SUFFIX)) {
				String prefix = fileName.replace(KEY_DIR_SUFFIX, "");
				key = prefix.substring(prefix.lastIndexOf(S3_PATH_SEP) + 1);
				keyToSize.put(key, 0l);
			}
			else {
				if (file.getFileName().endsWith(GZ_FILE_EXT)) {
					keyToSize.put(key, keyToSize.get(key) + file.getSize());
				}
			}
		}

		List<NodeToTask> nodeToFile = new ArrayList<>(nodes.size()); 
		for (Node node : nodes) {
			nodeToFile.add(new NodeToTask(node));
		}

		Map<String, Long> sortedMap = Utilities.sortByValue(keyToSize);
		for (String k : sortedMap.keySet()) {
			System.out.println(k);
			nodeToFile.get(0).addToTaskLst(k, false);
			nodeToFile.get(0).addToTotalSize(sortedMap.get(k));
			Collections.sort(nodeToFile);
		}

		for (NodeToTask node : nodeToFile) {
			System.out.println(node);
		}
	}

	*//**
	 * Rigourous Test :-)
	 *//*
	@SuppressWarnings("rawtypes")
	public void testApp()
	{
		try {
			Class<?> mapperClass = Class.forName(Testmapper.class.getName());
			Class<?> KEYIN = Class.forName(LongWritable.class.getName());
			Object keyIn = KEYIN.getConstructor(String.class).newInstance("1");
			Class<?> VALUEIN = Class.forName(Text.class.getName());
			Object valueIn = VALUEIN.getConstructor(String.class).newInstance("K");
			Mapper<?,?,?,?> mapper = (Mapper<?, ?, ?, ?>) mapperClass.newInstance();
			java.lang.reflect.Method mthd = mapperClass.getMethod("map", KEYIN, VALUEIN, Mapper.Context.class);
			mthd.invoke(mapper, keyIn, valueIn, mapper.new Context());
			List<Object> k = new ArrayList<>();
			k.add(valueIn);

			Reducer<?,?,?,?> reducer = (Reducer<?, ?, ?, ?>) TestReducer.class.newInstance();
			java.lang.reflect.Method mthdr = TestReducer.class.getMethod
					("reduce", VALUEIN, Iterable.class, Reducer.Context.class);
			mthdr.invoke(reducer, valueIn, k, reducer.new Context());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class Testmapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}
}

class TestReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		super.reduce(key, values, context);
	}
}

class NodeToTask implements Comparable<NodeToTask>{
	private Node node;
	private Long totalSize = new Long(0);
	private StringBuilder taskLst = new StringBuilder();

	public NodeToTask(Node node) {
		this.node = node;
	}

	public Node getNode() {
		return node;
	}

	public Long getTotalSize() {
		return totalSize;
	}

	public void addToTotalSize(Long totalSize) {
		this.totalSize += totalSize;
	}

	public String getTaskLst() {
		taskLst.deleteCharAt(taskLst.length() - 1);
		return taskLst.toString();
	}

	public void addToTaskLst(String taskName, boolean isFile) {
		if (isFile) {
			taskLst.append(taskName.substring(taskName.lastIndexOf("/"))).append(",");
		}
		else {
			taskLst.append(taskName).append(",");
		}
	}

	@Override
	public int compareTo(NodeToTask o) {
		return totalSize.compareTo(o.getTotalSize());
	}

	@Override
	public String toString() {
		return "NodeToTask [node=" + node + ", totalSize=" + totalSize
				+ ", taskLst=" + taskLst + "]";
	}*/
}
