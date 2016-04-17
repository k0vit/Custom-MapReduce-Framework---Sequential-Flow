package neu.edu.utilities;

import static org.apache.hadoop.Constants.FileNames.CLUSTER_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileNames.INSTANCE_DETAILS_FILE_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import neu.edu.mapreduce.common.Node;
public class Utilities {

	private Utilities() {};

	public static List<Node> readInstanceDetails() {
		List<Node> nodeLst = new ArrayList<>(10);
		try { 
			File instanceDetails = new File(INSTANCE_DETAILS_FILE_NAME);
			BufferedReader br = new BufferedReader(new FileReader(instanceDetails));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] details = line.split(",");
				Node node = new Node(details[1], details[2], details[3], String.valueOf(nodeLst.size() + 1));
				nodeLst.add(node);
			}
			br.close();
		}
		catch (Exception e) {
			// TODO
		}
		return nodeLst;
	}

	public static Properties readClusterProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(CLUSTER_PROP_FILE_NAME);
			prop.load(input);
			input.close();
		}
		catch (Exception e) {
			// TODO
		}
		return prop;
	}
}
