import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Log {

    private String logFilePath;
    private FileWriter fileWriter;

    public Log(String transactionId) {
        this.logFilePath = "./logs/" + transactionId + ".log";

        try {
            File logFile = new File(logFilePath);
            File logDir = logFile.getParentFile();

            if (!logDir.exists()) {
                if (logDir.mkdirs()) {
                    System.out.println("Created directory " + logDir.getPath());
                } else {
                    System.out.println("Failed to create directory " + logDir.getPath());
                    return;
                }
            }

            this.fileWriter = new FileWriter(logFilePath, true); // append mode
        } catch (IOException e) {
            System.out.println("Error opening transaction log file: " + e.getMessage());
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

    public void close() {
        try {
            fileWriter.close();
            // File logFile = new File(logFilePath);
            // if (logFile.delete()) {
            //     System.out.println("Deleted log file " + logFilePath);
            // } else {
            //     System.out.println("Failed to delete log file " + logFilePath);
            // }
        } catch (IOException e) {
            System.out.println("Error closing file writer: " + e.getMessage());
        }
    }
}
