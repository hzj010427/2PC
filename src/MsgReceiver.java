import java.util.concurrent.ConcurrentHashMap;

public class MsgReceiver implements Runnable {

    private ConcurrentHashMap<String, Transaction> transactions;
    private ProjectLib PL;

    public MsgReceiver(ConcurrentHashMap<String, Transaction> transactions, ProjectLib PL) {
        this.transactions = transactions;
        this.PL = PL;
    }

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
