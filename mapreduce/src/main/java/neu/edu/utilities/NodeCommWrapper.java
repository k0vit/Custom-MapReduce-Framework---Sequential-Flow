package neu.edu.utilities;

import java.util.logging.Logger;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Send the given data to the given ipAddress:portNo.
 * 
 * @author diptiSamant
 *
 */
public class NodeCommWrapper {

	public static void SendData(String clientIp, String port, String requestUrl, String data) {
		SendData(clientIp, port, requestUrl, data, "");
	}

	public static void SendData(String clientIp, String port, String requestUrl, String data, String fileName) {
		String address = "http://" + clientIp + ":" + port + "/" + requestUrl;
		log.info(String.format("[%s] Sending data to %s", fileName, address));
		// log.info("Sending data: " + data);
		try {
			Unirest.setTimeouts(10000, 120000);
			Unirest.post(address).body(data).asString();
		} catch (UnirestException e) {
			log.severe("[" + fileName + "] Exception sending post request: " + e.getMessage());
			log.severe("[" + fileName + "] RETRY sending file");
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e1) {
				log.info(e1.getMessage());
			}
			SendData(clientIp, port, requestUrl, data, fileName);
		}
	}

	private static final Logger log = Logger.getLogger(NodeCommWrapper.class.getName());
}
