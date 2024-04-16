import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the recovery of transactions that may not have completed due to system failures.
 * This class interacts with the system's log files to determine the state of incomplete
 * transactions and attempts to bring them to a consistent state by re-initiating their
 * respective commit protocols.
 */
public class RecoveryManager {

    private ProjectLib PL;

    /**
     * Constructs a RecoveryManager with a reference to ProjectLib for handling system-wide operations.
     *
     * @param PL The ProjectLib instance used for sending messages and other system interactions.
     */
    public RecoveryManager(ProjectLib PL) {
        this.PL = PL;
    }

    /**
     * Retrieves transactions that need to be recovered from log files. This method scans log files
     * in a predefined directory, interprets their content, and reconstructs transaction states.
     *
     * @return A concurrent hash map of transaction IDs to their corresponding Transaction objects that need recovery.
     */
    public ConcurrentHashMap<String, Transaction> getTransactions2Recover() {
        ConcurrentHashMap<String, Transaction> res = new ConcurrentHashMap<>();
        File logsDir = new File("./logs");
        File[] logFiles = logsDir.listFiles();

        if (logFiles == null) {
            return null;
        }

        for (File logFile : logFiles) {
            List<String> commitContent = parseLogParam(logFile);
            String transactionId = logFile.getName().split("\\.")[0];
            Transaction.Phase phase = parseLogStatus(logFile, transactionId);
            String fileName = commitContent.get(0);
            byte[] img = Base64.getDecoder().decode(commitContent.get(1));
            String[] sources = commitContent.get(2).split(",");

            Transaction t = new Transaction(transactionId, fileName, img, sources, PL);
            t.setPhase(phase);
            res.put(transactionId, t);
        }

        return res;
    }

    /**
     * Initiates the recovery process for a given transaction based on its last known phase.
     * Depending on the phase, it may re-ask for votes, commit, or abort the transaction.
     *
     * @param t The transaction to be recovered.
     */
    public void recover(Transaction t) {
        Transaction.Phase phase = t.getPhase();
        System.out.println("Recovering transaction: " + t.getID());

        switch (phase) {
            case PREPARE:
                t.askForVote();
                break;
            case ABORT:
                t.abort();
                break;
            case COMMIT:
                t.commit();
                break;
            default:
                System.out.println("error: fail to recover" + t.getID());
                break;
        }
    }

    /**
     * Reads a log file to find the last prepare response related to a specific transaction.
     * This helps in determining the state of the transaction during the prepare phase.
     *
     * @param logFile The log file to be read.
     * @param id      The transaction ID to search for in the log file.
     * @return The last prepare response found in the log file, or null if no such response exists.
     */
    public String getPrepareReply(File logFile, String id) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFile));
            String line;
            String prepareLine = null;
            while ((line = reader.readLine()) != null) {
                if ((line.contains("Yes") || line.contains("No")) && line.contains(id)) {
                    prepareLine = line;
                    reader.close();
                    break;
                }
            }
            return prepareLine;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error reading log file: " + logFile.getName());
        }
        return null;
    }

    /**
     * Reads a log file to find the last decision response related to a specific transaction.
     * This is used to confirm whether all nodes acknowledged the final decision of the transaction.
     *
     * @param logFile The log file to be read.
     * @param id      The transaction ID to search for in the log file.
     * @return The last decision response found in the log file, or null if no such response exists.
     */
    public String getDecision(File logFile, String id) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFile));
            String line;
            String ackLine = null;
            while ((line = reader.readLine()) != null) {
                if (line.contains("ACK") && line.contains(id)) {
                    ackLine = line;
                    reader.close();
                    break;
                }
            }
            return ackLine;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error reading log file: " + logFile.getName());
        }
        return null;
    }

    /**
     * Parses a log file to determine the last known phase of a transaction based on log entries.
     *
     * @param logFile The file containing the log entries.
     * @param id      The ID of the transaction to determine the phase for.
     * @return The last known phase of the transaction as recorded in the log file.
     */
    private Transaction.Phase parseLogStatus(File logFile, String id) {
        Transaction.Phase res = null;
        String latestPhase = null;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFile));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("phase") && line.contains(id)) {
                    latestPhase = line.split(",")[0].split(":")[1].trim();
                    System.out.println("latest phase: " + latestPhase + " for transaction " + id);

                    switch (latestPhase) {
                        case "prepare":
                            res = Transaction.Phase.PREPARE;
                            break;
                        case "commit":
                            res = Transaction.Phase.COMMIT;
                            break;
                        case "abort":
                            res = Transaction.Phase.ABORT;
                            break;
                        default:
                            System.out.println("Error parsing log file: " + logFile.getName());
                            break;
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error reading log file: " + logFile.getName());
        }
        return res;
    }

    /**
     * Parses the initial parameters of a transaction from a log file.
     * These parameters typically include the file name, image data, and source nodes.
     *
     * @param logFile The file to parse the parameters from.
     * @return A list of strings representing the transaction parameters.
     */
    private List<String> parseLogParam(File logFile) {
        List<String> params = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(logFile));
            String line = reader.readLine();
            if (line != null) {
                String[] paramTokens = line.split("-");
                for (String paramToken : paramTokens) {
                    params.add(paramToken.trim());
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error reading log file: " + logFile.getName());
        }

        return params;
    }
}
