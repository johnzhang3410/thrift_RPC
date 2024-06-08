import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client {
	static String FE_host;
	static int FE_port;

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: java Client FE_host FE_port");
			System.exit(-1);
		}

		FE_host = args[0];
		FE_port = Integer.parseInt(args[1]);

		try {
			// Unit tests
			simpleTest();
			testEmptyBatch();
			testEmptyPassword();

			// Stress Tests
			try {
				stressTest(2, 2, (short) 10);
			} catch (InterruptedException e) {
				System.out.println("Exception while running stress test: " + e.toString());
			}
		} catch (TException x) {
			x.printStackTrace();
		}
	}

	public static void simpleTest() throws TException {
		TSocket sock = new TSocket(FE_host, FE_port);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);
		transport.open();

		List<String> password = new ArrayList<>();
		password.add("hello");
		password.add("world");
		List<String> hash = client.hashPassword(password, (short) 10);
		System.out.println("Password: " + password.get(0));
		System.out.println("Hash: " + hash.get(0));
		System.out.println("Positive check: " + client.checkPassword(password, hash));
		hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
		System.out.println("Negative check: " + client.checkPassword(password, hash));
		try {
			hash.set(0, "too short");
			List<Boolean> rets = client.checkPassword(password, hash);
			System.out.println("Exception check failed: no exception thrown");
		} catch (Exception e) {
			System.out.println("Exception check passed: exception thrown");
		}
		System.out.println("Simple test passed!");
		transport.close();
	}

	public static void testEmptyPassword() throws TException {
		System.out.println("Running empty password test");

		TSocket sock = new TSocket(FE_host, FE_port);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);
		transport.open();

		List<String> password = new ArrayList<>();
		password.add("");

		try {
			List<String> hash = client.hashPassword(password, (short) 10);
			List<Boolean> rets = client.checkPassword(password, hash);
			assert rets.get(0) == true;
		} catch (Exception e) {
			System.out.println("Empty Password test failed, excpetion thrown: " + e.toString());
			return;
		}

		transport.close();
		System.out.println("Empty Password test passed!");
	}

	public static void testEmptyBatch() throws TException {
		TSocket sock = new TSocket(FE_host, FE_port);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);
		transport.open();

		List<String> password = new ArrayList<>();

		try {
			List<String> hash = client.hashPassword(password, (short) 10);
			List<Boolean> rets = client.checkPassword(password, hash);
			assert hash.size() == 0 && rets.size() == 0;
		} catch (Exception e) {
			System.out.println("Empty Password test failed, excpetion thrown: " + e.toString());
			return;
		}

		transport.close();
		System.out.println("Empty Batch test passed!");
	}

	public static void stressTest(int numThreads, int batchSize, short logRounds)
			throws InterruptedException, TException {
		System.out.println("Running strees test");

		Runnable task = new Runnable() {
			@Override
			public void run() {
				List<String> passwords = generateRandomPasswords(batchSize);
				try {
					TSocket sock = new TSocket(FE_host, FE_port);
					TTransport transport = new TFramedTransport(sock);
					TProtocol protocol = new TBinaryProtocol(transport);
					BcryptService.Client client = new BcryptService.Client(protocol);
					transport.open();

					List<String> hash = client.hashPassword(passwords, logRounds);

					assert passwords.size() == hash.size();

					List<Boolean> results = client.checkPassword(passwords, hash);
					for (Boolean b : results) {
						assert b == true;
					}

					transport.close();
				} catch (TException e) {
					System.out.println("Exception caught running thread: " + e.toString());
				}
			}
		};

		Thread[] threads = new Thread[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threads[i] = new Thread(task);
			threads[i].start();
		}

		for (int i = 0; i < numThreads; i++) {
			threads[i].join();
		}

		System.out.println("Stress test passed!");
	}

	public static List<String> generateRandomPasswords(int batchSize) {
		Random random = new Random();
		String[] passwords = new String[batchSize];

		for (int i = 0; i < passwords.length; i++) {
			int stringLength = random.nextInt(1025);
			StringBuilder builder = new StringBuilder();

			for (int j = 0; j < stringLength; j++) {
				// random ASCII character from space (32) to tilde (126)
				char randomChar = (char) (random.nextInt(95) + 32);
				builder.append(randomChar);
			}

			passwords[i] = builder.toString();
		}

		return Arrays.asList(passwords);
	}
}