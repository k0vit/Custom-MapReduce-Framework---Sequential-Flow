package neu.edu.utilities;

import static org.apache.hadoop.Constants.CommProperties.DEFAULT_DATA;
import static org.apache.hadoop.Constants.CommProperties.DEFAULT_PORT;

import java.util.logging.Logger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
/**
 * Send the given data to the given ipAddress:portNo.
 * 
 */
public class NodeCommWrapper {

	public static void sendData(String nodeIp, String requestUrl) {
		sendData(nodeIp, DEFAULT_PORT, requestUrl, DEFAULT_DATA);
	}
	
	public static void sendData(String nodeIp, String requestUrl, String data) {
		sendData(nodeIp, DEFAULT_PORT, requestUrl, data);
	}

	public static void sendData(String nodeIp, String port, String requestUrl, String data) {
		String address = "http://" + nodeIp + ":" + port + requestUrl;
		log.info(String.format("Posting %s to %s", data, address));
		try {
			//Unirest.setTimeouts(10000, 120000);
			HttpResponse<String> resp = Unirest.post(address).body(data).asString();
			log.info("Response recieved with body as " + resp.getBody() + " with status as " + resp.getStatus());
		} catch (UnirestException e) {
			log.severe("Exception sending post request: " + e.getMessage());
			log.severe("RETRY sending file");
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e1) {
				log.info(e1.getMessage());
			}
			sendData(nodeIp, port, requestUrl, data);
		}
	}

	private static final Logger log = Logger.getLogger(NodeCommWrapper.class.getName());
}
