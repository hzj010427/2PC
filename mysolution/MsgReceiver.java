import java.util.concurrent.ConcurrentHashMap;

/**
 * A class that acts as a receiver for messages related to transactions within a distributed system.
 * It implements {@link Runnable} to allow it to run in a separate thread, constantly listening for
 * and processing incoming messages via a specified instance of {@link ProjectLib}.
 */
public class MsgReceiver implements Runnable {

    private ConcurrentHashMap<String, Transaction> transactions;
    private ProjectLib PL;

    /**
     * Constructs a MsgReceiver with a map of transactions and a project library instance.
     * This setup enables the MsgReceiver to access and update transactions based on the incoming messages.
     *
     * @param transactions A concurrent hash map of transactions indexed by transaction IDs.
     *                     This map allows the receiver to fetch and update the status of transactions as messages are processed.
     * @param PL           An instance of ProjectLib used for receiving messages from other nodes or processes in the system.
     */
    public MsgReceiver(ConcurrentHashMap<String, Transaction> transactions, ProjectLib PL) {
        this.transactions = transactions;
        this.PL = PL;
    }

    /**
     * When executed by a thread, this method continuously listens for new messages from the project library.
     * Upon receiving a message, it performs the following actions:
     * - Parses the message to determine the transaction ID.
     * - Retrieves the corresponding Transaction object from the map.
     * - Updates the transaction's response time to the current time.
     * - Calls the transaction's handleRes method to process the message based on the transaction's current state.
     * 
     * This method ensures that each transaction is updated with incoming data as soon as it arrives, maintaining
     * the responsiveness and accuracy of the system's transaction handling.
     */
    @Override
    public void run() {
        while (true) {
            ProjectLib.Message msg = PL.getMessage();
            String parts[] = new String(msg.body).split(":", 2);
            String transactionId = parts[0];
            Transaction transaction = transactions.get(transactionId);
            transaction.setResponseTime(System.currentTimeMillis());
            transaction.handleRes(msg);
        }
    }
}
