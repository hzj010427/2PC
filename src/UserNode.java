import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;

/**
 * Represents a user node in a distributed system that handles messages related to file operations,
 * including locking, logging, and transaction decisions. It implements the ProjectLib.MessageHandling
 * interface to receive and process messages according to a two-phase commit protocol.
 */
public class UserNode implements ProjectLib.MessageHandling {

	private final String myId;
	private final HashMap<String, FileLock> lockedFiles = new HashMap<>();
	private final HashMap<String, Log> WALs = new HashMap<>();
	private static RecoveryManager rm;
	private static ProjectLib PL;
	
	/**
     * Constructor that initializes the UserNode with a given identifier.
     * 
     * @param id The unique identifier for this node.
     */
	public UserNode(String id) {
		myId = id;
	}

	/**
     * Receives and handles messages from other nodes or processes.
     * This method processes 'prepare' messages for phase-1 and 'decision' messages for phase-2 of
     * the transaction protocol.
     *
     * @param msg The message received.
     * @return true Always returns true to indicate successful handling of the message.
     */
	@Override
	public boolean deliverMessage(ProjectLib.Message msg) {
		// System.out.println(myId + ": Got message from " + msg.addr);
		String parsedMsg = new String(msg.body);

		if (parsedMsg.startsWith("prepare")) { // phase-1
			handlePrepare(msg);
		} else if (parsedMsg.startsWith("decision")) { // phase-2
			handleDecision(msg);
		} else {
			System.out.println(myId + ": Unknown message received");
		}

		return true;
	}

	/**
     * Handles the 'prepare' phase of a transaction by deciding whether to lock the required
     * resources and asking the user for confirmation to proceed.
     *
     * @param msg The 'prepare' message containing details about the transaction and the resources involved.
     */
	private void handlePrepare(ProjectLib.Message msg) {
		String[] parts = new String(msg.body).split(":", 4);
		String transactionId = parts[1];
		String files[] = parts[2].split(",");
		byte[] image = Base64.getDecoder().decode(parts[3]);
		boolean userDecision = false;
		String res = null;
		Log WAL = getWAL(transactionId);
		// WAL.write2Log("id: " + transactionId + ", source: " + msg.addr + ", content: prepare");

		/* get response from the log */
		res = rm.getPrepareReply(WAL.getLogFile(), transactionId);
		if (res != null) {
			PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
			return;
		}

		synchronized (lockedFiles) {
			if (checkFilesExists(files) && !checkFilesOccupied(files)) {
				try {
					lockResources(files);
					userDecision = PL.askUser(image, files);
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println(myId + ": Error while locking resources");
				}
			}
	
			if (!userDecision) {
				try {
					releaseResources(files);
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println(myId + ": Error while releasing resources");
				}
			}
		}

		res = userDecision ? transactionId + ":Yes" : transactionId + ":No";
		WAL.write2Log(res);
		PL.fsync();
		PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
	}

	/**
     * Handles the 'decision' phase of a transaction, which includes committing or aborting
     * the transaction based on the received decision.
     *
     * @param msg The 'decision' message containing the final decision and details about the transaction.
     */
	private void handleDecision(ProjectLib.Message msg) {
		String[] parts = new String(msg.body).split(":", 4);
		String transactionId = parts[1];
		String decision = parts[2];
		String files[] = parts[3].split(",");
		Log WAL = getWAL(transactionId);

		/* get response from the log */
		String res = rm.getDecision(WAL.getLogFile(), transactionId);
		if (res != null) {
			PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
			return;
		}

		synchronized (lockedFiles) {
			try {
				if (decision.equals("commit")) {
					res = transactionId + ":ACK";
					WAL.write2Log(res);
					PL.fsync();
					deleteFiles(files);
					releaseResources(files);
					PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
				} else if (decision.equals("abort")) {
					releaseResources(files);
				} else {
					System.out.println(myId + ": Unknown decision received");
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println(myId + ": Error while releasing resources");
			}
		}
	}

	/**
	 * Checks if the given files exist.
	 * 
	 * @param files The list of file paths to check.
	 * @return true if all files exist, false otherwise.
	 */
	private boolean checkFilesExists(String files[]) {
		for (String file : files) {
			File f = new File(file);
			if (!f.exists() || f.isDirectory()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks if the given files are currently locked.
	 * 
	 * @param files The list of file paths to check.
	 * @return true if any file is locked, false otherwise.
	 */
	private boolean checkFilesOccupied(String files[]) {
		for (String file : files) {
			if (lockedFiles.containsKey(file)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Deletes the given files.
	 * 
	 * @param files The list of file paths to delete.
	 */
	private void deleteFiles(String files[]) {
		for (String file : files) {
			File f = new File(file);
			f.delete();
		}
	}

	/**
	 * Locks the given files.
	 * 
	 * @param files The list of file paths to lock.
	 * @throws IOException If an I/O error occurs while locking the files.
	 */
	private void lockResources(String files[]) throws IOException {
		for (String file : files) {
			RandomAccessFile raFile = new RandomAccessFile(file, "rw");
			FileChannel fileChannel = raFile.getChannel();
			FileLock lock = fileChannel.lock();
			lockedFiles.put(file, lock);
		}
	}

	/**
	 * Releases the locks on the given files.
	 * 
	 * @param files The list of file paths to release.
	 * @throws IOException If an I/O error occurs while releasing the locks.
	 */
	private void releaseResources(String files[]) throws IOException {
		for (String file : files) {
			if (lockedFiles.containsKey(file)) {
				lockedFiles.get(file).release();
				lockedFiles.remove(file);
			}
		}
	}

	/**
	 * Retrieves the Write-Ahead Log (WAL) for the given transaction.
	 * 
	 * @param transactionId The unique identifier for the transaction.
	 * @return The WAL for the transaction.
	 */
	private Log getWAL(String transactionId) {
		if (!WALs.containsKey(transactionId)) {
			WALs.put(transactionId, new Log(transactionId));
		}
		return WALs.get(transactionId);
	}
	
	public static void main (String args[]) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);
		rm = new RecoveryManager(PL);
		System.out.println("UserNode " + args[1] + " start");
	}
}
