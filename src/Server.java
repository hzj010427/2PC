import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a server that manages transaction operations in a distributed system.
 * This server handles initiating and recovering transactions, and maintains a map
 * of ongoing transactions. It uses a two-phase commit protocol to ensure data consistency
 * across different nodes involved in a transaction.
 */
public class Server implements ProjectLib.CommitServing {

	private static ProjectLib PL;
	private static ConcurrentHashMap<String, Transaction> transactionMap = new ConcurrentHashMap<>();

	/**
     * Starts a new transaction and asks all involved nodes to vote on the commit.
     * This is the initial step in the two-phase commit protocol where the server prepares
     * the transaction, logs it, and requests votes from all nodes.
     *
     * @param filename The name of the file involved in the transaction.
     * @param img      The image data related to the transaction in byte array format.
     * @param sources  Array of strings representing the source nodes and their associated files.
     */
	public void startCommit(String filename, byte[] img, String[] sources) {
		String transactionId = UUID.randomUUID().toString();
		Transaction transaction = new Transaction(transactionId, filename, img, sources, PL);
		transactionMap.put(transactionId, transaction);
		Log WAL = transaction.getWAL();
		String imgBase64 = Base64.getEncoder().encodeToString(img);
		WAL.write2Log(filename + "-" + imgBase64 + "-" + String.join(",", sources));
		transaction.askForVote();
		PL.fsync();
	}
	
	/**
     * The main method to start the server. It initializes the server, recovers any incomplete transactions,
     * and starts threads to handle message receiving and sending. It also handles exceptions and ensures
     * the server is ready to process transactions.
     *
     * @param args Command line arguments expecting a single argument for the port number.
     * @throws Exception Throws an Exception if the required command line argument is not provided.
     */
	public static void main (String args[]) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib(Integer.parseInt(args[0]), srv);
		System.out.println("Server start");

		/* recover transactions */
		RecoveryManager rm = new RecoveryManager(PL);
		if (rm.getTransactions2Recover() == null) {
			System.out.println("No transactions to recover");
		} else {
			transactionMap = rm.getTransactions2Recover();
			
			for (Transaction transaction : transactionMap.values()) {
				System.out.println(transaction.getID());
				rm.recover(transaction);
			}
		}

		/* start threads to receive and send messages */
		Thread receiver = new Thread(new MsgReceiver(transactionMap, PL));
		Thread sender = new Thread(new MsgSender(transactionMap, PL));

		receiver.start();
		sender.start();

		try {
			receiver.join();
			sender.join();
		} catch (InterruptedException e) {
			System.out.println("Main thread interrupted: " + e.getMessage());
		}
	}
}
