package neu.edu.mapreduce.common;

/**
 * Information about the cluster ec2 instances
 * 
 * @author kovit
 *
 */
public class Node {

	private String privateIp;
	private String publicIp;
	private boolean isSlave;
	private String id;
	
	public String getPrivateIp() {
		return privateIp;
	}
	public String getPublicIp() {
		return publicIp;
	}
	public boolean isSlave() {
		return isSlave;
	}
	
	public Node(String privateIp, String publicIp, String isSlave, String id) {
		super();
		this.privateIp = privateIp;
		this.publicIp = publicIp;
		this.isSlave = isSlave.equals("S") ? true : false;
		this.id = id;
	}
	
	public String getId() {
		return id;
	}
	
	@Override
	public String toString() {
		return "Node [privateIp=" + privateIp + ", publicIp=" + publicIp
				+ ", isSlave=" + isSlave + ", id=" + id + "]";
	}
}
