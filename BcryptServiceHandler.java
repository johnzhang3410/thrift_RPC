import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
	static Logger log;
	// var for round robin scheduling
	private static int currentBackendIndex = 0;

	List<BackendNode> backendNodes = new ArrayList<>();

	static {
		BasicConfigurator.configure();
		log = Logger.getLogger(BcryptServiceHandler.class);
	}

	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
		if (logRounds < 4 || logRounds > 31) {
			throw new IllegalArgument("logRounds out of range. Must be between 4 and 31.");
		}

		log.info("GOT INTO HASHPASSWORD FUNCTION AT FE");

		if (!backendNodes.isEmpty()) {
			return forwardToBackend(password, logRounds);
		} else {
			// if no backend node is available
			// hashes all passwords in the list at FE
			List<String> hashedPasswords = new ArrayList<>();
			for (String pwd : password) {
				try {
					String hashed = BCrypt.hashpw(pwd, BCrypt.gensalt(logRounds));
					hashedPasswords.add(hashed);
				} catch (Exception e) {
					throw new IllegalArgument(e.getMessage());
				}
			}
			return hashedPasswords;
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash)
			throws IllegalArgument, org.apache.thrift.TException {
		if (password.size() != hash.size()) {
			throw new IllegalArgument("The number of passwords does not match the number of hashes.");
		}

		log.info("GOT INTO CHECKPASSWORD FUNCTION AT FE");

		if (!backendNodes.isEmpty()) {
			return forwardCheckToBackend(password, hash);
		} else {
			// TODO: logRounds
			// if no backend node is available
			// checks all passwords in the list at FEç
			List<Boolean> ret = new ArrayList<>();
			for (int i = 0; i < password.size(); i++) {
				try {
					String onePwd = password.get(i);
					String oneHash = hash.get(i);
					ret.add(BCrypt.checkpw(onePwd, oneHash));
				} catch (Exception e) {
					throw new IllegalArgument(e.getMessage());
				}
			}
			return ret;
		}
	}

	// TODO: send a request to backend handler to handle hashPassword
	private List<String> forwardToBackend(List<String> passwords, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
		// selects a backend node after load balancing
		BackendNode node = selectBackendNode();
		List<String> result = new ArrayList<>();

		TSocket socket = new TSocket(node.getHost(), node.getPort());
		TTransport transport = new TFramedTransport(socket);

		try {
			// opens up a client to backend node that is at host and port
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			log.info("password being passed in is " + passwords);
			log.info("connecting to backend node at " + node.getHost() + ":" + node.getPort());

			// calls backend hashPassword
			result = client.hashPassword(passwords, logRounds);

		} catch (IllegalArgument e) {
			log.info("IllegalArgument exception thrown at frontend forwardToBackend");
			throw e;
		} catch (TException x) {
			log.error("TException thrown in forwardToBackend", x);
			throw x;
		} finally {
			log.info("Got into the finally block in forwardToBackend");
			if (transport != null) {
				transport.close(); // Ensure transport is closed regardless of exceptions
			}
		}

		return result;
	}

	// TODO: send a requets to backend handler to handle checkPassword
	private List<Boolean> forwardCheckToBackend(List<String> passwords, List<String> hashes)
			throws IllegalArgument, org.apache.thrift.TException {
		BackendNode node = selectBackendNode();
		List<Boolean> result = new ArrayList<>();

		TSocket socket = new TSocket(node.getHost(), node.getPort());
		TTransport transport = new TFramedTransport(socket);

		try {
			// TSocket socket = new TSocket(node.getHost(), node.getPort());
			// TTransport transport = new TFramedTransport(socket);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			log.info("Forwarding to BE node at " + node.getHost() + ":" + node.getPort());
			log.info("Passwords being passed: " + passwords);
			log.info("Hashes being passed: " + hashes);

			result = client.checkPassword(passwords, hashes);

		} catch (IllegalArgument e) {
			log.info("exception thrown at frontend forwardCheckToBackend");
			throw e;
		} catch (TException x) {
			log.error("Error in forwarding checkPassword", x);
			throw x;
		} finally {
			log.info("Got into the finally block in forwardCheckToBackend");
			if (transport != null) {
				transport.close(); // Ensure transport is closed regardless of exceptions
			}
		}

		return result;
	}

	// TODO: method for load balancing, round robin
	private BackendNode selectBackendNode() {
		BackendNode node = backendNodes.get(currentBackendIndex);
		currentBackendIndex = (currentBackendIndex + 1) % backendNodes.size();
		return node;
	}

	// BE node can call this to register itself for FE node to see
	@Override
	public void registerBackend(String host, int port) {
		BackendNode node = new BackendNode(host, port);
		backendNodes.add(node);
		log.info("Registered BE node at " + host + ":" + port);
	}

}
