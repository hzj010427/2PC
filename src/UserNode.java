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
	public final String myId;
	public final HashMap<String, FileLock> lockedFiles = new HashMap<>();
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
		String files[] = 
			new String(msg.body).split(":", 2)[1].split(";", 2)[0].split(",");
		byte[] image = 
			Base64.getDecoder().decode(new String(msg.body).split(":", 2)[1].split(";", 2)[1]); // decode image
		boolean fileExists = checkFilesExists(files);
		boolean userDecision = false;
		String res = null;

		if (fileExists) {
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

		res = userDecision ? "Yes" : "No";
		PL.sendMessage(new ProjectLib.Message(msg.addr, res.getBytes()));
	}

	private void handleDecision(ProjectLib.Message msg) {
		String decision = new String(msg.body).split(":", 2)[1].split(";", 2)[0];
		String files[] = new String(msg.body).split(":", 2)[1].split(";", 2)[1].split(",");

		try {
			if (decision.equals("commit")) {
				deleteFiles(files);
				releaseResources(files);
				PL.sendMessage(new ProjectLib.Message(msg.addr, "ACK".getBytes()));
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

	private boolean checkFilesExists(String files[]) {
		for (String file : files) {
			File f = new File(file);
			if (!f.exists() || f.isDirectory()) {
				return false;
			}
		}
		return true;
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
	
	public static void main ( String args[]) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);
	}
}
