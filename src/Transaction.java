import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a transaction in a distributed system that handles a two-phase commit protocol. 
 * This class manages the transaction's lifecycle, which includes the preparation, commit, 
 * abort phases, and handles responses from different nodes involved in the transaction.
 */
public class Transaction {

	private static final int TIMEOUT = 6000; // 6 seconds

    private String id;
    private ProjectLib PL;
	private Map<String, List<String>> sourceMap;
    private Map<String, Boolean> nodeRes;
	private Phase phase;
	private byte[] image;
	private String fileName;
	private long startTime;
	private long responseTime;
	private Log WAL;

	/**
     * Constructs a Transaction object with specific details needed to process it.
     *
     * @param id       Unique identifier for this transaction.
     * @param fileName The name of the file associated with the transaction.
     * @param img      The image data related to the transaction in byte array format.
     * @param sources  Array of strings representing the source nodes and associated files.
     * @param PL       Reference to the ProjectLib instance for communication purposes.
     */
    public Transaction(String id, String fileName, byte[] img, String[] sources, ProjectLib PL) {
		this.PL = PL;
        this.id = id;
        this.fileName = fileName;
        this.image = img;
        this.sourceMap = parseSources(sources);
        this.phase = Phase.PREPARE;
        this.nodeRes = new HashMap<>();
		this.WAL = new Log(id);
    }

	/**
     * Initiates the prepare phase of the transaction by asking all participating nodes to vote.
     */
    public void askForVote() {
		startTime = System.currentTimeMillis(); // start timer before sending prepare message
        String imgBase64 = Base64.getEncoder().encodeToString(image);
		WAL.write2Log("phase: prepare" + ", id: " + id);

        for (String node : sourceMap.keySet()) {
            String msg = "prepare:" + id + ":" + String.join(",", sourceMap.get(node)) + ":" + imgBase64;
			// WAL.write2Log("dest: " + node + ", content: prepare");
            PL.sendMessage(new ProjectLib.Message(node, msg.getBytes()));
        }
    }

	/**
     * Re-initiates the prepare phase by resending the vote request to all participating nodes.
     */
	public void reAskForVote() {
		String imgBase64 = Base64.getEncoder().encodeToString(image);
		for (String node : sourceMap.keySet()) {
            String msg = "prepare:" + id + ":" + String.join(",", sourceMap.get(node)) + ":" + imgBase64;
			// WAL.write2Log("dest: " + node + ", content: prepare");
            PL.sendMessage(new ProjectLib.Message(node, msg.getBytes()));
        }
	}

	/**
     * Handles responses received from nodes during the transaction. This method processes responses based on the current phase of the transaction.
     *
     * @param msg The message received from a node.
     */
    public synchronized void handleRes(ProjectLib.Message msg) {
        switch (phase) {
            case PREPARE:
                handlePrepareRes(msg);
                break;
            case COMMIT:
            case ABORT:
                handleDecisionRes(msg);
                break;
            default:
                break;
        }
    }

	/**
     * Sets the response time, typically used for timeout calculations.
     *
     * @param time The time of the response in milliseconds.
     */
	public void setResponseTime(long time) {
		responseTime = time;
	}
    
	/**
     * Commits the transaction by sending a commit message to all nodes involved in the transaction.
     */
    public void commit() {
        for (String node : sourceMap.keySet()) {
            String msg2Send = "decision:" + id + ":" + "commit:" + String.join(",", sourceMap.get(node));
            // WAL.write2Log("dest: " + node + ", content: commit");
            PL.sendMessage(new ProjectLib.Message(node, msg2Send.getBytes()));
        }
        phase = Phase.COMMIT;
		WAL.write2Log("phase: commit" + ", id: " + id);
    }

	/**
     * Aborts the transaction by sending an abort message to all nodes involved in the transaction.
     */
    public void abort() {
        for (String node : sourceMap.keySet()) {
            String msg2Send = "decision:" + id + ":" + "abort:" + String.join(",", sourceMap.get(node));
            // WAL.write2Log("dest: " + node + ", content: abort");
            PL.sendMessage(new ProjectLib.Message(node, msg2Send.getBytes()));
        }
        phase = Phase.ABORT;
		WAL.write2Log("phase: abort" + ", id: " + id);
    }

	/**
     * Processes responses received during the prepare phase of the transaction.
     *
     * @param msg The prepare phase response message from a node.
     */
    public void handlePrepareRes(ProjectLib.Message msg) {
		String res = new String(msg.body).split(":", 2)[1];
		System.out.println("Received prepare response from " + msg.addr + " Content: " + res + " id: " + id);
		// WAL.write2Log("source: " + msg.addr + ", content: " + res);

		if (res.equals("Yes") || res.equals("No")) {
			WAL.write2Log("phase: prepare" + ", id: " + id);
			nodeRes.put(msg.addr, res.equals("Yes"));

			boolean shouldCommit = !isTimeout() && recvAllRes() && allYes();
			boolean shouldAbort = isTimeout() || (recvAllRes() && !allYes());

			if (shouldCommit) {
				commit();
			} else if (shouldAbort) {
				abort();
			}

			if (shouldCommit || shouldAbort) {
				nodeRes.clear(); // clear the responses for the next phase
			}

			PL.fsync(); // flush the responses to stable storage
		} else {
			System.out.println(id + ": drop message in prepare phase");
		}
	}

	/**
     * Processes responses received during the decision phase of the transaction.
     *
     * @param msg The decision phase response message from a node.
     */
    public void handleDecisionRes(ProjectLib.Message msg) {
		String res = new String(msg.body).split(":", 2)[1];
		String msg2Log = "Received decision response from " + msg.addr + " Content: " + res + " id: " + id;
		System.out.println(msg2Log);
		// WAL.write2Log("source: " + msg.addr + ", content: " + res);

		if (res.equals("ACK")) {
			nodeRes.put(msg.addr, true);
			if (recvAllRes()) {
				System.out.println(id + ": All nodes have acknowledged");
				write2Dir(fileName, image);
				phase = Phase.DONE;
				// WAL.write2Log("phase: done" + ", id: " + id);
				WAL.close();
				PL.fsync(); // flush the messages when all nodes have acknowledged
			}
		} else {
			System.out.println(id + ": drop message in decision phase");
		}
    }

	/**
     * Writes the image data to the specified directory on the disk.
     *
     * @param file The file path where the image is to be written.
     * @param img  The image data to write.
     */
    private void write2Dir(String file, byte[] img) {
		try {
			Files.write(Paths.get(file), img);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Server: Error while writing image to disk");
		}
	}
    
	/**
     * Parses the source information to map each node to the files it handles.
     *
     * @param sources Array of strings representing nodes and their associated files.
     * @return A map of nodes to their lists of files.
     */
    private Map<String, List<String>> parseSources(String[] sources) {
		Map<String, List<String>> sourceMap = new HashMap<>();
		for (String source : sources) {
			String[] parts = source.split(":", 2);
			String node = parts[0];
			String fileName = parts[1];
			if (!sourceMap.containsKey(node)) {
				sourceMap.put(node, new ArrayList<>());
			}
			sourceMap.get(node).add(fileName);
		}
		return sourceMap;
	}

	/**
     * Checks if the response time has exceeded the defined timeout.
     *
     * @return true if the current time minus the start time is greater than the timeout, otherwise false.
     */
	private boolean isTimeout() {
		return responseTime - startTime > TIMEOUT;
	}

	/**
     * Determines if all nodes have responded positively ('Yes') to the transaction.
     *
     * @return true if all responses are 'Yes', otherwise false.
     */
	private boolean allYes() {
		return nodeRes.values().stream().allMatch(decision -> decision.equals(Boolean.TRUE));
	}

	/**
     * Checks if all responses have been received from all nodes involved in the transaction.
     *
     * @return true if all responses have been received, otherwise false.
     */
    public boolean recvAllRes() {
		for (String node : sourceMap.keySet()) {
			if (!nodeRes.containsKey(node)) {
				return false;
			}
		}
		return true;
	}

	/**
     * Enumeration defining the possible phases of a transaction.
     */
    public enum Phase {
		PREPARE,
        COMMIT,
        ABORT,
		DONE
	}

	/* some getters and setters */
	public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public Phase getPhase() {
        return phase;
    }

    public String getID() {
        return id;
    }

    public Log getWAL() {
        return WAL;
    }

	public long getStartTime() {
		return startTime;
	}
}
