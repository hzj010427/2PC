import java.util.concurrent.ConcurrentHashMap;

public class MsgSender implements Runnable {

    private static final int PULSE = 2000; //ms
    private static final int TIMEOUT = 6000; //ms

    private ConcurrentHashMap<String, Transaction> transactions;
    private ProjectLib PL;

    public MsgSender(ConcurrentHashMap<String, Transaction> transactions, ProjectLib PL) {
        this.transactions = transactions;
        this.PL = PL;
    }

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
