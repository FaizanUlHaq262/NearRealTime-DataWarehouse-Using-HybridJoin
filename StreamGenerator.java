import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

public class StreamGenerator extends Thread {
    BlockingQueue<List<tuple>> streamQueue;
    // private int offset = 0; // Tracks the number of rows already read
    boolean running = true;
    private int batchSize; // Number of rows to read in one go
    private int speedFactor = 1; // Default speed factor
    private int MAX_PACE = 15000; // Maximum speed factor
    private int MIN_PACE = 500; // Minimum speed factor
    private Connection conn;
    private int counter = 0;

    private int start = 0;
    // private int end = 1000;

    public StreamGenerator(BlockingQueue<List<tuple>> streamQueue , Connection conn , int batchSize) {
        this.streamQueue = streamQueue;
        this.conn = conn;
        this.batchSize = batchSize;
    }
        /*SPEED FUNCTIONS WILL BE ACCESSED BY THE CONTROLLER THREAD IN THE OTHER FILE */
    public void increasePace() {
        speedFactor = Math.min(speedFactor + 300, MAX_PACE); // Avoid exceeding maximum pace
    }

    public void decreasePace() {
        speedFactor = Math.max(speedFactor - 300, MIN_PACE); // Avoid pace going below minimum
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                // ArrayListValuedHashMap<String, String> dataChunk = readNextChunk();
                List<tuple> dataChunk = readChunks();
                if (dataChunk != null) {
                    streamQueue.put(dataChunk);
                } else {
                    // No more data to read
                    running = false;        // Stop the thread
                }
            } catch (InterruptedException e) {
                running = false;
                Thread.currentThread().interrupt();
            }
            // try {
            //     // sleep(1000);        //sleep for 2 seconds
            // } catch (InterruptedException e) {
            //     // TODO Auto-generated catch block
            //     e.printStackTrace();
            // }
        }
    }

    public void stopStreaming() {
        running = false;
    }

    private List<tuple> readChunks(){
        List<tuple> dataChunk = new ArrayList<>();
        String query = "SELECT * FROM transactions ORDER BY `Order ID` ASC LIMIT ?,?";
        try (PreparedStatement ps = conn.prepareStatement(query)){
            ps.setInt(1, start);      //initial batchSize * speedFactor = 1000 * 1 = 1000
            ps.setInt(2, batchSize);

            try(ResultSet rs = ps.executeQuery()){
                if(!rs.isBeforeFirst()){//this helps check if the whole table has been read or not
                    return null;
                }
                while(rs.next()){
                    tuple t = new tuple(rs);
                    dataChunk.add(t);
                }
                if(dataChunk.isEmpty()){
                    return null;
                }
                System.out.println("Read from "+ start +" to " + batchSize + " Iteration==>" + counter++ );
                start = batchSize;
                batchSize = batchSize+speedFactor;
                // //print values of tuples in list here 
                // int c = 0;
                // for(tuple t : dataChunk){
                //     System.out.println(t.getOrderID() + " " + t.getOrderDate() + " " + t.getProductID() + " " + t.getCustomerID() + " " + t.getCustomerName() + " " + t.getGender() + " " + t.getQuantity());

                //     c++;
                //     if(c == 3){
                //         break;
                //     }
                // }

                // try {
                //     sleep(50000);
                // } catch (InterruptedException e) {
                //     // TODO Auto-generated catch block
                //     e.printStackTrace();
                // }
                rs.close();
            }

        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return dataChunk;
    }

}
