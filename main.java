import java.sql.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

public class main {
    public static void main(String[] args) {
        // Use an array to hold the connections
        final Connection[] connections = new Connection[3];

        try {
            // Load JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            Scanner scanner = new Scanner(System.in);
            System.out.println("Do you want to use default connection values? (y/n)");
            String input = scanner.nextLine();

            if (input.equals("y") || input.equals("Y")) {

            // Establish database connections
            connections[0] = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/transactions", "root", "root"); // Transactions
            connections[1] = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/masterdata", "root", "root"); // Master Data
            connections[2] = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/electronica-dw", "root", "root"); // Data Warehouse

            }else if (input.equals("n") || input.equals("N")) {
                String tran = System.console().readLine("Enter the transaction database URL: ");
                String tranPass = System.console().readLine("Enter the transaction database password: ");
                String tranUser = System.console().readLine("Enter the transaction database username: ");

                String master = System.console().readLine("Enter the master database URL: ");
                String masterPass = System.console().readLine("Enter the master database password: ");
                String masterUser = System.console().readLine("Enter the master database username: ");

                String dw = System.console().readLine("Enter the data warehouse database URL: ");
                String dwPass = System.console().readLine("Enter the data warehouse database password: ");
                String dwUser = System.console().readLine("Enter the data warehouse database username: ");

                // Establish database connections
                connections[0] = DriverManager.getConnection(tran, tranUser, tranPass); // Transactions
                connections[1] = DriverManager.getConnection(master, masterUser, masterPass); // Master Data
                connections[2] = DriverManager.getConnection(dw, dwUser, dwPass); // Data Warehouse
            }else {
                System.out.println("Invalid input. Exiting...");
                System.exit(0);

            }

            BlockingQueue<List<tuple>> streamQueue = new ArrayBlockingQueue<>(20000);
            StreamGenerator streamGenerator = new StreamGenerator(streamQueue, connections[0], 1000);
            HybridJoinThread hybridJoinThread = new HybridJoinThread(connections[1], connections[2], streamQueue);
            Controller controller = new Controller(streamGenerator, hybridJoinThread, streamQueue);


            streamGenerator.start();
            hybridJoinThread.start();
            controller.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {		// This is a hook that will be executed when the program is terminated
                streamGenerator.stopStreaming();
                controller.stopController();
                hybridJoinThread.interrupt();

                for (Connection conn : connections) {
                    closeConnection(conn);
                }
            }));

        } catch (ClassNotFoundException e) {
            System.err.println("MySQL JDBC Driver not found.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error connecting to the database.");
            e.printStackTrace();
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
