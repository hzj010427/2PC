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

public class Server implements ProjectLib.CommitServing {

	private static ProjectLib PL;
	private static ConcurrentHashMap<String, Transaction> transactionMap = new ConcurrentHashMap<>();

	public void startCommit(String filename, byte[] img, String[] sources) {
		String transactionId = UUID.randomUUID().toString();
		Transaction transaction = new Transaction(transactionId, filename, img, sources, PL);
		transactionMap.put(transactionId, transaction);
		Log WAL = transaction.getWAL();
		String imgBase64 = Base64.getEncoder().encodeToString(img);
		WAL.write2Log(filename + "-" + imgBase64 + "-" + String.join(",", sources));
		transaction.askForVote();
	}
		
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
