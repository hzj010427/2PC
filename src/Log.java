import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Log {

    private String logFilePath;
    private FileWriter fileWriter;
    private File logFile;
    private static final Object lock = new Object();

    public Log(String transactionId) {
        initLogFile(transactionId);
    }

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
    
    public synchronized void write2Log(String message) {
        try {
            fileWriter.write(message);
            fileWriter.write(System.lineSeparator());
            fileWriter.flush();
        } catch (IOException e) {
            System.out.println("Error writing to transaction log: " + e.getMessage());
        }
    }

    public File getLogFile() {
        return logFile;
    }

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
