package neu.edu.mr.manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

/**
 * Creates ec2-isntances readign cluster.properties
 * @author kovit
 *
 */
public class ClusterCreator {

	private ClusterParams params;
	private AmazonEC2Client amazonEC2Client;
	private final static Logger LOGGER = Main.logger;

	public ClusterCreator(ClusterParams params) {
		this.params = params;
		amazonEC2Client = new AmazonEC2Client(new BasicAWSCredentials(params.getAccessKey(), params.getSecretKey()));
		amazonEC2Client.withRegion(Regions.US_EAST_1);
	}

	public boolean createCluster() {
		try {
			createKey();
			createSecurityGroup();

			// code to create cluster instances
			RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
			runInstancesRequest.withImageId(params.getBaseImageName())
			.withInstanceType(params.getInstanceFlavor())
			.withMinCount(1)
			.withMaxCount(params.getNoOfInstance() + 1) // +1 for master
			.withKeyName(Main.keyName)
			.withSecurityGroups(Main.securityGrpName);

			LOGGER.log(Level.FINE, "Creating instance with parameters " + 
					params.getBaseImageName() + ", " + params.getInstanceFlavor() + ", " + params.getNoOfInstance()
					+ ", " + Main.keyName);
			RunInstancesResult result = amazonEC2Client.runInstances(runInstancesRequest);
			writeInstanceDetails(result.getReservation().getReservationId());
			
			//amazonEC2Client.shutdown();

			return true;
		}
		catch (Exception e) {
			System.err.println("Cluster creation failed. Reason:" + e.getMessage());
			return false;
		}
	}

	// writes instance details to file, like private and public ip.
	// Check InstnaceDetails.csv
	private void writeInstanceDetails(String reservationId) {
		try {
			File file = new File(Main.CLUSTER_DETAILS_FILE_NAME);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			LOGGER.log(Level.FINE, "Fetching instances for reservation id " + reservationId);
			boolean masterNodeCreated = false;
			// checks the progress of cluster instnace creation
			for (Reservation reservation: amazonEC2Client.describeInstances().getReservations()) {
				if (reservation.getReservationId().equals(reservationId)) {
					for (Instance inst : reservation.getInstances()) {
						LOGGER.log(Level.FINE, "Waiting for instance with id " + inst.getImageId());
						LOGGER.log(Level.FINE, "Current State " + inst.getState().getName());
						DescribeInstancesRequest statusReq = new DescribeInstancesRequest();
						statusReq.withInstanceIds(inst.getInstanceId());
						String state = inst.getState().getName();
						while (!state.equals(InstanceStateName.Running.toString())) {
							Thread.sleep(10000);
							LOGGER.log(Level.FINE, "Sleeping for 10 seconds. State not changed");
							DescribeInstancesResult result = amazonEC2Client.describeInstances(statusReq);
							if (result.getReservations().size() > 0) {
								inst = result.getReservations().get(0).getInstances().get(0);
								state = inst.getState().getName();
								LOGGER.log(Level.FINE, "State changed to " + state);
							}
						}

						// create InstnaceDetail.csv with all isntance details
						StringBuilder instanceDetails = new StringBuilder();
						instanceDetails.append(inst.getInstanceId()).append(",");
						instanceDetails.append(inst.getPrivateIpAddress()).append(",");
						instanceDetails.append(inst.getPublicIpAddress()).append(",");
						if (!masterNodeCreated) {
							instanceDetails.append('M').append(System.lineSeparator());
							masterNodeCreated = true;
						}
						else {
							instanceDetails.append('S').append(System.lineSeparator());
						}
						LOGGER.log(Level.FINE, "Instance details: " + instanceDetails.toString());
						bw.write(instanceDetails.toString());
					}
				}
			}
			bw.close(); 
		}
		catch (Exception e) {
			System.err.println("Failed to write instance details to file. Reason " + e.getMessage());
			System.exit(-1);
		}
	}

	private void createKey() {
		CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
		createKeyPairRequest.withKeyName(Main.keyName);
		CreateKeyPairResult createKeyPairResult = amazonEC2Client.createKeyPair(createKeyPairRequest);
		KeyPair keyPair = createKeyPairResult.getKeyPair();
		saveToFile(keyPair.getKeyMaterial());

		LOGGER.log(Level.INFO, "The key was created name = " + keyPair.getKeyName() + 
				"\nIts fingerprint is=" + keyPair.getKeyFingerprint()
				+ "\nIts material is= \n" + keyPair.getKeyMaterial());
	}

	private void saveToFile(String privateKey) {
		try {
			File file = new File(Main.EC2_KEY_FILE_NAME);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(privateKey);
			bw.close();
		}
		catch (Exception e) {
			System.err.println("Failed to save private key. Please save the private key found in the "
					+ Main.CLUSTER_MANAGER_LOGGER + " file in the current directory as " 
					+ Main.EC2_KEY_FILE_NAME);
		}
	}

	private void createSecurityGroup() {
		try {
			CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
			csgr.withGroupName(Main.securityGrpName).withDescription("a9 security grp");
			amazonEC2Client.createSecurityGroup(csgr);
			LOGGER.log(Level.FINE, "Created security grp with name a9SecurityGrp");

			AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest();
			ingressRequest.withGroupName(Main.securityGrpName).withIpPermissions(getIpPermissions());
			amazonEC2Client.authorizeSecurityGroupIngress(ingressRequest);
			LOGGER.log(Level.FINE, "Setting ingress rule allowing any ip address for all tcp flow");
		} 
		catch (AmazonServiceException e) {
			System.err.println("Failed to create security grp " + e.getErrorMessage());
			System.exit(-1);
		}
	}

	// TODO: check if security grp can be made more secure
	private List<IpPermission> getIpPermissions() {
		List<IpPermission> permissions = new ArrayList<IpPermission>();

		// supporting all the ips
		List<String> ipRanges = new ArrayList<String>(1);
		ipRanges.add("0.0.0.0/0");

		// on port 4567 client and sort node communicates with each other and 22 for ssh
		IpPermission internalComm = new IpPermission();
		internalComm.setIpProtocol("tcp");
		internalComm.setFromPort(0);
		internalComm.setToPort(65535);
		internalComm.setIpRanges(ipRanges);

		permissions.add(internalComm);
		return permissions;
	}
}
