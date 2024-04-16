import java.util.concurrent.ConcurrentHashMap;

/**
 * A class designed to periodically resend messages for transactions in a distributed system.
 * This is particularly important in scenarios where responses from nodes are not received
 * within expected time frames. It implements the {@link Runnable} interface, allowing it to be
 * executed by a thread to continuously check transaction states and resend messages accordingly.
 */
public class MsgSender implements Runnable {

    private static final int PULSE = 2000; //ms
    private static final int TIMEOUT = 6000; //ms

    private ConcurrentHashMap<String, Transaction> transactions;
    private ProjectLib PL;

    /**
     * Constructs a message sender for handling and resending transaction messages.
     *
     * @param transactions A concurrent hash map of transactions that may need message resending.
     * @param PL           An instance of ProjectLib used for sending messages within the system.
     */
    public MsgSender(ConcurrentHashMap<String, Transaction> transactions, ProjectLib PL) {
        this.transactions = transactions;
        this.PL = PL;
    }

    /**
     * When run within a thread, this method periodically checks each transaction to determine
     * if all responses have been received. If not, it decides based on the transaction phase and
     * elapsed time whether to resend a request for votes, commit, or abort messages.
     * 
     * This process helps ensure that transactions do not stall due to missed or delayed responses
     * from participating nodes.
     */
    // TODO: 已经有回复的node不再发送
    @Override  
    public void run() {
        try {
            while (true) {
                for (Transaction transaction : transactions.values()) {
                    if (!transaction.recvAllRes()) {
                        if (transaction.getPhase() == Transaction.Phase.PREPARE) {
                            long timeElapsed = System.currentTimeMillis() - transaction.getStartTime();
                            System.out.println(transaction.getID() + " time elapsed: " + timeElapsed);
                            if (timeElapsed > TIMEOUT) {
                                transaction.abort();
                                System.out.println("resending message: timeout aborting transaction " + transaction.getID());
                            } else {
                                transaction.reAskForVote();
                                System.out.println("resending message: asking for vote for transaction " + transaction.getID());
                            }
                        } else if (transaction.getPhase() == Transaction.Phase.COMMIT) {
                            transaction.commit();
                            System.out.println("resending message: committing transaction " + transaction.getID());
                        } else if (transaction.getPhase() == Transaction.Phase.ABORT) {
                            transaction.abort();
                            System.out.println("resending message: aborting transaction " + transaction.getID());
                        } else {
                            System.out.println("error: fail to send message");
                        }
                    }
                }

                Thread.sleep(PULSE);
            }
        } catch (InterruptedException e) {
            System.out.println("TaskRunner interrupted: " + e.getMessage());
        }
    }
}
