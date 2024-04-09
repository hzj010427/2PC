import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server implements ProjectLib.CommitServing {

	public static ProjectLib PL;
	public static Map<String, List<String>> sourceMap;
	public static Phase phase;
	public static byte[] image;
	public static String file;

	public void startCommit(String filename, byte[] img, String[] sources) {
		System.out.println("Server: Got request to commit " + filename);
		String imgBase64 = Base64.getEncoder().encodeToString(img); // encode image to base64
		sourceMap = parseSources(sources);
		phase = Phase.PREPARE;
		image = img;
		file = filename;

		for (String node : sourceMap.keySet()) {
			String msg = "prepare:" + String.join(",", sourceMap.get(node)) + ";" + imgBase64;
			PL.sendMessage(new ProjectLib.Message(node, msg.getBytes()));
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

	private static boolean recvAllRes(Map<String, Boolean> nodeRes) {
		for (String node : sourceMap.keySet()) {
			if (!nodeRes.containsKey(node)) {
				return false;
			}
		}
		return true;
	}

	private static void write2Dir(String file, byte[] img) {
		try {
			Files.write(Paths.get(file), img);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Server: Error while writing image to disk");
		}
	}

	private static void handlePrepareRes(ProjectLib.Message msg, Map<String, Boolean> nodeRes) {
		String msgBody = new String(msg.body);
		System.out.println("Server: Got message from " + msg.addr + " Content: " + msgBody);

		if (msgBody.equals("Yes") || msgBody.equals("No")) {
			nodeRes.put(msg.addr, msgBody.equals("Yes"));

			if (recvAllRes(nodeRes)) {
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

	private static void handleDecisionRes(ProjectLib.Message msg, Map<String, Boolean> nodeRes) {
		String msgBody = new String(msg.body);
		System.out.println("Server: Got message from " + msg.addr + " Content: " + msgBody);

		if (msgBody.equals("ACK")) {
			nodeRes.put(msg.addr, true);
			if (recvAllRes(nodeRes)) {
				System.out.println("Server: All nodes have acknowledged");
				write2Dir(file, image);
			} 
		} else {
			System.out.println("Server: Unknown message received");
		}
	}

	private enum Phase {
		PREPARE,
		DECISION
	}
	
	public static void main (String args[]) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib(Integer.parseInt(args[0]), srv);
		Map<String, Boolean> nodeRes = new HashMap<>();
		
		while (true) {
			ProjectLib.Message msg = PL.getMessage();

			if (phase == Phase.PREPARE) {
				handlePrepareRes(msg, nodeRes);
			} else if (phase == Phase.DECISION) {
				handleDecisionRes(msg, nodeRes);
			} else {
				System.out.println("Server: Unknown phase");
			}
		}
	}
}
