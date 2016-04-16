package neu.edu.mapreduce.common;

public class Node {

	private String privateIp;
	private String publicIp;
	private boolean isSlave;
	
	public String getPrivateIp() {
		return privateIp;
	}
	public String getPublicIp() {
		return publicIp;
	}
	public boolean isSlave() {
		return isSlave;
	}
	
	public Node(String privateIp, String publicIp, boolean isSlave) {
		super();
		this.privateIp = privateIp;
		this.publicIp = publicIp;
		this.isSlave = isSlave;
	}
}
