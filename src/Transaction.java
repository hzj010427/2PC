import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction {
    private String id;
    private ProjectLib PL;
	private Map<String, List<String>> sourceMap;
    private Map<String, Boolean> nodeRes;
	private Phase phase;
	private byte[] image;
	private String fileName;

    public Transaction(String id, String fileName, byte[] img, String[] sources) {
        this.id = id;
        this.fileName = fileName;
        this.image = img;
        this.sourceMap = parseSources(sources);
        this.phase = Phase.PREPARE;
        this.nodeRes = new HashMap<>();
    }

    public void askForVote() {
        String imgBase64 = Base64.getEncoder().encodeToString(image); // encode image to base64
        for (String node : sourceMap.keySet()) {
            String msg = "prepare:" + String.join(",", sourceMap.get(node)) + ";" + imgBase64;
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

    private void handlePrepareRes(ProjectLib.Message msg) {
		String msgBody = new String(msg.body);
		System.out.println("Server: Got message from " + msg.addr + " Content: " + msgBody, " id: ", id);

		if (msgBody.equals("Yes") || msgBody.equals("No")) {
			nodeRes.put(msg.addr, msgBody.equals("Yes"));

			if (recvAllRes()) {
				if (nodeRes.values().stream().allMatch(decision -> decision.equals(Boolean.TRUE))) {
					for (String node : sourceMap.keySet()) {
						String msgToSend = "decision:commit;" + String.join(",", sourceMap.get(node));
						PL.sendMessage(new ProjectLib.Message(node, msgToSend.getBytes()));
					}
				} else {
					for (String node : sourceMap.keySet()) {
						String msgToSend = "decision:abort;" + String.join(",", sourceMap.get(node));
						PL.sendMessage(new ProjectLib.Message(node, msgToSend.getBytes()));
					}
				}

				phase = Phase.DECISION;
				nodeRes.clear(); // reset nodeRes for next phase
			}
		} else {
			System.out.println("Server: Unknown message received");
		}
	}

    private void handleDecisionRes(ProjectLib.Message msg, Map<String, Boolean> nodeRes) {
		String msgBody = new String(msg.body);
		System.out.println("Server: Got message from " + msg.addr + " Content: " + msgBody, " id: ", id);

		if (msgBody.equals("ACK")) {
			nodeRes.put(msg.addr, true);
			if (recvAllRes()) {
				System.out.println("Server: All nodes have acknowledged");
				write2Dir(fileName, image);
			} 
		} else {
			System.out.println("Server: Unknown message received");
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

    private enum Phase {
		PREPARE,
		DECISION
	}
}
