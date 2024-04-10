import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Server implements ProjectLib.CommitServing {

	public static ProjectLib PL;
	public static Map<String, Transaction> transactionMap = new HashMap<>();

	public void startCommit(String filename, byte[] img, String[] sources) {
		System.out.println("Server: Got request to commit " + filename);
		String transactionId = UUID.randomUUID().toString();
		Transaction transaction = new Transaction(transactionId, filename, img, sources, PL);
		transactionMap.put(transactionId, transaction);
		transaction.askForVote();
	}
	
	public static void main (String args[]) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib(Integer.parseInt(args[0]), srv);
		
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			String parts[] = new String(msg.body).split(":", 2);
			String transactionId = parts[0];
			Transaction transaction = transactionMap.get(transactionId);
			transaction.handleRes(msg);
		}
	}
}
