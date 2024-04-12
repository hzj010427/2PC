import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public Transaction(String id, String fileName, byte[] img, String[] sources, ProjectLib PL) {
		this.PL = PL;
        this.id = id;
        this.fileName = fileName;
        this.image = img;
        this.sourceMap = parseSources(sources);
        this.phase = Phase.PREPARE;
        this.nodeRes = new HashMap<>();
		this.WAL = new Log(id);
		WAL.write2Log("id: " + id + ", user nodes: " + String.join(",", sourceMap.keySet()));
    }

    public void askForVote() {
		startTime = System.currentTimeMillis(); // start timer before sending prepare message
        String imgBase64 = Base64.getEncoder().encodeToString(image);
		WAL.write2Log("phase: prepare");

        for (String node : sourceMap.keySet()) {
            String msg = "prepare:" + id + ":" + String.join(",", sourceMap.get(node)) + ":" + imgBase64;
			WAL.write2Log("dest: " + node + ", content: prepare");
            PL.sendMessage(new ProjectLib.Message(node, msg.getBytes()));
        }
    }

    public void handleRes(ProjectLib.Message msg) {
        switch (phase) {
            case PREPARE:
                handlePrepareRes(msg);
                break;
            case DECISION:
                handleDecisionRes(msg);
                break;
            default:
                break;
        }
    }

	public void setResponseTime(long time) {
		responseTime = time;
	}	

    private void handlePrepareRes(ProjectLib.Message msg) {
		String res = new String(msg.body).split(":", 2)[1];
		System.out.println("Received prepare response from " + msg.addr + " Content: " + res + " id: " + id);
		WAL.write2Log("source: " + msg.addr + ", content: " + res);

		if (res.equals("Yes") || res.equals("No")) {
			nodeRes.put(msg.addr, res.equals("Yes"));

			boolean shouldCommit = !isTimeout() && recvAllRes() && allYes();
			boolean shouldAbort = isTimeout() || (recvAllRes() && !allYes());

			if (shouldCommit) {
				for (String node : sourceMap.keySet()) {
					String msg2Send = "decision:" + id + ":" + "commit:" + String.join(",", sourceMap.get(node));
					WAL.write2Log("dest: " + node + ", content: commit");
					PL.sendMessage(new ProjectLib.Message(node, msg2Send.getBytes()));
				}
			} else if (shouldAbort) {
				for (String node : sourceMap.keySet()) {
					String msg2Send = "decision:" + id + ":" + "abort:" + String.join(",", sourceMap.get(node));
					WAL.write2Log("dest: " + node + ", content: abort");
					PL.sendMessage(new ProjectLib.Message(node, msg2Send.getBytes()));
				}
			}

			if (shouldCommit || shouldAbort) {
				phase = Phase.DECISION;
				WAL.write2Log("phase: " + (shouldCommit ? "commit" : "abort"));
				PL.fsync(); // flush the messages before changing phase
				nodeRes.clear(); // clear the responses for the next phase
			}
		} else {
			System.out.println(id + ": drop message in prepare phase");
		}
	}

    private void handleDecisionRes(ProjectLib.Message msg) {
		String res = new String(msg.body).split(":", 2)[1];
		String msg2Log = "Received decision response from " + msg.addr + " Content: " + res + " id: " + id;
		System.out.println(msg2Log);
		WAL.write2Log("source: " + msg.addr + ", content: " + res);

		if (res.equals("ACK")) {
			nodeRes.put(msg.addr, true);
			if (recvAllRes()) {
				System.out.println(id + ": All nodes have acknowledged");
				write2Dir(fileName, image);
				WAL.write2Log("phase: done");
				WAL.close();
				PL.fsync(); // flush the messages when all nodes have acknowledged
			} 
		} else {
			System.out.println(id + ": drop message in decision phase");
		}
    }

    private void write2Dir(String file, byte[] img) {
		try {
			Files.write(Paths.get(file), img);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Server: Error while writing image to disk");
		}
	}
    
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

    private boolean recvAllRes() {
		for (String node : sourceMap.keySet()) {
			if (!nodeRes.containsKey(node)) {
				return false;
			}
		}
		return true;
	}

	private boolean isTimeout() {
		return responseTime - startTime > TIMEOUT;
	}

	private boolean allYes() {
		return nodeRes.values().stream().allMatch(decision -> decision.equals(Boolean.TRUE));
	}

    private enum Phase {
		PREPARE,
		DECISION
	}
}
