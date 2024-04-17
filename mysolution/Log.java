import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Manages the logging activities for transactions within a distributed system. This class is responsible
 * for creating log files, writing to them, and safely closing and deleting them upon the completion
 * of transactions. It ensures that all transaction-related activities are logged for recovery and
 * auditing purposes.
 */
public class Log {

    private String logFilePath;
    private FileWriter fileWriter;
    private File logFile;
    private static final Object lock = new Object();

    /**
     * Constructs a Log object associated with a specific transaction ID.
     * This constructor initializes the log file for the transaction.
     *
     * @param transactionId The ID of the transaction for which the log is being created.
     */
    public Log(String transactionId) {
        initLogFile(transactionId);
    }

    /**
     * Initializes the log file for the transaction. This method creates a directory and log file if they do not exist.
     * It handles the creation and opening of the log file safely to ensure that log data can be written without interruption.
     *
     * @param transactionId The transaction ID for which the log file is being initialized.
     */
    private void initLogFile(String transactionId) {
        logFilePath = "./logs/" + transactionId + ".log";
        
        synchronized (lock) {
            try {
                logFile = new File(logFilePath);
                File logDir = logFile.getParentFile();
        
                if (!logDir.exists()) {
                    if (logDir.mkdirs()) {
                        System.out.println("Created directory " + logDir.getPath());
                    } else {
                        System.out.println("Failed to create directory " + logDir.getPath());
                        return;
                    }
                }
        
                fileWriter = new FileWriter(logFilePath, true); // append mode
            } catch (IOException e) {
                System.out.println("Error opening transaction log file: " + e.getMessage());
            }
        }
    }
    
    /**
     * Writes a message to the transaction log file. This method is synchronized to ensure thread safety
     * when multiple threads are writing to the log file simultaneously.
     *
     * @param message The message to be written to the log.
     */
    public synchronized void write2Log(String message) {
        try {
            fileWriter.write(message);
            fileWriter.write(System.lineSeparator());
            fileWriter.flush();
        } catch (IOException e) {
            System.out.println("Error writing to transaction log: " + e.getMessage());
        }
    }

    /**
     * Retrieves the log file associated with this Log instance.
     *
     * @return The File object representing the log file.
     */
    public File getLogFile() {
        return logFile;
    }

    /**
     * Closes the FileWriter and deletes the log file. This method is typically called when a transaction
     * is complete and the log file is no longer needed. It ensures that resources are freed and the
     * system's integrity is maintained by removing unnecessary log files.
     */
    public void close() {
        try {
            fileWriter.close();
            File logFile = new File(logFilePath);
            if (logFile.delete()) {
                System.out.println("Deleted log file " + logFilePath);
            } else {
                System.out.println("Failed to delete log file " + logFilePath);
            }
        } catch (IOException e) {
            System.out.println("Error closing file writer: " + e.getMessage());
        }
    }
}
