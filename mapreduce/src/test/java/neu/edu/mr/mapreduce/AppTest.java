package neu.edu.mr.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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

	/**
	 * Rigourous Test :-)
	 */
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
			List<Text> k = new ArrayList<>();
			k.add(new Text("value"));

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
