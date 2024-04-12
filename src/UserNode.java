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

public class UserNode implements ProjectLib.MessageHandling {

	private final String myId;
	private final HashMap<String, FileLock> lockedFiles = new HashMap<>();
	private final HashMap<String, Log> WALs = new HashMap<>();
	public static ProjectLib PL;
	
	public UserNode(String id) {
		myId = id;
	}

	@Override
	public boolean deliverMessage(ProjectLib.Message msg) {
		System.out.println(myId + ": Got message from " + msg.addr);
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

	private void handlePrepare(ProjectLib.Message msg) {
		String[] parts = new String(msg.body).split(":", 4);
		String transactionId = parts[1];
		String files[] = parts[2].split(",");
		byte[] image = Base64.getDecoder().decode(parts[3]);
		boolean userDecision = false;
		String res = null;
		Log WAL = getWAL(transactionId);
		WAL.write2Log("id: " + transactionId + ", source: " + msg.addr + ", content: prepare");

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
		WAL.write2Log("response: " + (userDecision ? "Yes" : "No"));
		PL.fsync();
		PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
	}

	private void handleDecision(ProjectLib.Message msg) {
		String[] parts = new String(msg.body).split(":", 4);
		String transactionId = parts[1];
		String decision = parts[2];
		String files[] = parts[3].split(",");
		Log WAL = getWAL(transactionId);
		WAL.write2Log("id: " + transactionId + ", source: " + msg.addr + ", content: " + decision);

		synchronized (lockedFiles) {
			try {
				if (decision.equals("commit")) {
					deleteFiles(files);
					releaseResources(files);
					String res = transactionId + ":ACK";
					WAL.write2Log("response: ACK");
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

	private boolean checkFilesExists(String files[]) {
		for (String file : files) {
			File f = new File(file);
			if (!f.exists() || f.isDirectory()) {
				return false;
			}
		}
		return true;
	}

	private boolean checkFilesOccupied(String files[]) {
		for (String file : files) {
			if (lockedFiles.containsKey(file)) {
				return true;
			}
		}
		return false;
	}

	private void deleteFiles(String files[]) {
		for (String file : files) {
			File f = new File(file);
			f.delete();
		}
	}

	private void lockResources(String files[]) throws IOException {
		for (String file : files) {
			RandomAccessFile raFile = new RandomAccessFile(file, "rw");
			FileChannel fileChannel = raFile.getChannel();
			FileLock lock = fileChannel.lock();
			lockedFiles.put(file, lock);
		}
	}

	private void releaseResources(String files[]) throws IOException {
		for (String file : files) {
			if (lockedFiles.containsKey(file)) {
				lockedFiles.get(file).release();
				lockedFiles.remove(file);
			}
		}
	}

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
	}
}
