import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

public class HybridJoinThread extends Thread {
/*variables */
    private Connection connMD; // Connection to Master Data
    private Connection connDW; // Connection to Data Warehouse
    private BlockingQueue<List<tuple>> streamQueue;
    private ArrayListValuedHashMap<String, tuple> hashTable = new ArrayListValuedHashMap<>();
    private DoublyLinkedList linkedList = new DoublyLinkedList();
    private int counter = 0;
/*functions */
    public HybridJoinThread(Connection connMD, Connection connDW, 
                            BlockingQueue<List<tuple>> streamQueue) {
        this.connMD = connMD;
        this.connDW = connDW;
        this.streamQueue = streamQueue;
    }

    public static double doubleMaker(String s) {
        String firstPart = s.substring(0, s.indexOf("$"));
        double d = Double.parseDouble(firstPart);
        return d;
    }

    private int calculateSales(int quantityOrdered, double productPrice) {
        return (int) (quantityOrdered * productPrice);
    }
/*code */
    @Override
    public void run() {
        try {
            while (!interrupted()) {
                hybridJoin(streamQueue.take());
            }
        } catch (InterruptedException | ParseException e) {
            Thread.currentThread().interrupt();
        }
    }
    private void hybridJoin(List<tuple> streamData) throws ParseException{
        for (tuple t : streamData) {
            String productID = t.getProductID();
            linkedList.addNode(productID);          //keeps inserting the productID into the doubly linked list queue
            hashTable.put(productID, t);            //keeps inserting all the data with same productID into the hash table 
        }
        //once all the productIDs are inserted into the doubly linked list and hash table, we can start the hybrid join
        //get the first productID from the doubly linked list queue and then load master data for that productID
        //then delete the productID from the doubly linked list queue and hash table
        //then get the next productID from the doubly linked list queue and load master data for that productID
        //then delete the productID from the doubly linked list queue and hash table
        //and so on until the doubly linked list queue is empty
        while (linkedList.head != null) {
            String productID = linkedList.head.joinAttribute;
            loadChunkFromMasterData(productID);
            // deleteFromHashTableAndQueue(productID);
        }


    }
   
    private void loadChunkFromMasterData(String joinAttribute) throws ParseException {
        try (PreparedStatement ps = connMD.prepareStatement("SELECT * FROM master_data WHERE ProductID = ?")) {
            ps.setString(1, joinAttribute);
            try (ResultSet rsMD = ps.executeQuery()) {
                if (rsMD.next()) {
                    putInWarehouse(joinAttribute, rsMD);
                    deleteFromHashTableAndQueue(joinAttribute);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void putInWarehouse(String joinAtt , ResultSet masterData) throws SQLException , ParseException{
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy");
        format.setLenient(false);

        // System.out.println("HASHTABLE:\n"+ hashTable + "\nGET FUNCTION\n"+hashTable.get(joinAtt));
        //PRINT EVERY VALUE IN THE HASH TABLE
        if (hashTable != null){
            for (String key : hashTable.keySet()) {     //for all of the keys available in the hashtable
                for(tuple t1 : hashTable.get(key)){     //FOR EVERY TUPLE IN THE LIST OF TUPLES
                        // System.out.println(t1.getOrderID() + " " + t1.getOrderDate() + " " + t1.getProductID() + " " + t1.getCustomerID() + " " + t1.getCustomerName() + " " + t1.getGender() + " " + t1.getQuantity());
                        // System.out.println("JOIN ATTRIBUTE: " + joinAtt + " " + masterData.getString("ProductID"));
                        if(t1.getProductID().equals(masterData.getString("productID"))){     //if the productID in the tuple is equal to the productID in the master data
                            String orderDate = t1.getOrderDate();
                            String[] splitDate = orderDate.split(" ");
                            if (splitDate.length > 0) {
                                orderDate = splitDate[0];
                            }
                            try {
                                if (orderDate.split("/")[2].length() >= 3) {
                                    return;
                                } else {
                                    format.parse(orderDate);
                                }
                            } catch (ParseException e) {
                                return;
                            }

                            /*INSERTING DATA INTO WAREHOUSE */
                            if (checkDate(orderDate) == false) {
                                //skip the insertion into the warehouse
                                return;
                            }else{
                                int supplierID = masterData.getInt("supplierID");
                                String supplierName = masterData.getString("supplierName");
                    
                                // Check if the supplier ID is already in use
                                // if (isSupplierIdInUse(supplierID)) {
                                //     // Assign a new unique supplier ID
                                //     supplierID = getNewSupplierId();
                                // }
                                
                                
                                insertTimeDataFromOrderDate(orderDate);     //inserting into time dimension

                                try {
                                    insertIntoProductDim(Integer.parseInt(joinAtt), masterData.getString("productName"), doubleMaker(masterData.getString("productPrice")));
                                } catch (SQLException e) {
                                    System.out.println("SQL Exception: " + e.getMessage());
                                    System.err.println("Error inserting into product dimension" + masterData.getString("productName") + " " + doubleMaker(masterData.getString("productPrice")));
                                }
                                try {
                                    insertIntoSupplierDim(supplierID, supplierName);
                                } catch (SQLException e) {
                                    System.out.println("SQL Exception: " + e.getMessage());
                                }
                                try {
                                    insertIntoCustomerDim(Integer.parseInt(t1.getCustomerID()), t1.getCustomerName(), t1.getGender());
                                } catch (SQLException e) {
                                    System.out.println("SQL Exception: " + e.getMessage());
                                }
                                try {
                                    insertIntoStoreDim(masterData.getInt("storeID"), masterData.getString("storeName"));
                                } catch (SQLException e) {
                                    System.out.println("SQL Exception: " + e.getMessage());
                                }

                                try (PreparedStatement ps = connDW.prepareStatement(
                                    "INSERT INTO Transactions_Fact (OrderID, OrderDate, ProductID, CustomerID, SupplierID, StoreID, Sales) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                                    ps.setInt(1, Integer.parseInt(t1.getOrderID()));
                                    java.sql.Date temp = cleanDate(orderDate);
                                    // System.out.println(orderDate + " " + java.sql.Date.valueOf(orderDate));
                                    ps.setDate(2, temp);
                                    ps.setInt(3, Integer.parseInt(joinAtt));
                                    ps.setInt(4, Integer.parseInt(t1.getCustomerID()));
                                    ps.setInt(5, masterData.getInt("supplierID"));
                                    ps.setInt(6, masterData.getInt("storeID"));
                                    ps.setInt(7, calculateSales(t1.getQuantity(), doubleMaker(masterData.getString("productPrice"))));
                                    ps.executeUpdate();
                                } catch (SQLException e){
                                    System.out.println("SQL Exception: " + e);
                                }
                            }
                        }
                }    
            } 
            // //stop thread
            // System.out.println("Thread Stopped");
            // Thread.currentThread().interrupt();
        }
    }

    private void deleteFromHashTableAndQueue(String joinAttribute) {
        // Delete the product ID from the doubly linked list queue
        linkedList.deleteNode(joinAttribute);
        // Delete the product ID and all the values corresponding to it from the hash table
        hashTable.remove(joinAttribute);
    }

                        /*INSERTION FUNCTIONS */
    private void insertIntoProductDim(int productId, String productName, double productPrice) throws SQLException {
        String sql = "INSERT INTO PRODUCT_DIM (ProductID, ProductName, ProductPrice) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connDW.prepareStatement(sql)) {
            ps.setInt(1, productId);
            ps.setString(2, productName);
            ps.setDouble(3, productPrice);
            ps.executeUpdate();
        }
    }

    private void insertIntoSupplierDim(int supplierId, String supplierName) throws SQLException {
        String sql = "INSERT INTO SUPPLIER_DIM (SupplierID, SupplierName) VALUES (?, ?)";
        try (PreparedStatement ps = connDW.prepareStatement(sql)) {
            ps.setInt(1, supplierId);
            ps.setString(2, supplierName);
            ps.executeUpdate();
        }
    }

    private void insertIntoCustomerDim(int customerId, String customerName, String gender) throws SQLException {
        String sql = "INSERT INTO CUSTOMER_DIM (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connDW.prepareStatement(sql)) {
            ps.setInt(1, customerId);
            ps.setString(2, customerName);
            ps.setString(3, gender);
            ps.executeUpdate();
        }
    }

    private void insertIntoStoreDim(int storeId, String storeName) throws SQLException {
        String sql = "INSERT INTO STORE_DIM (StoreID, StoreName) VALUES (?, ?)";
        try (PreparedStatement ps = connDW.prepareStatement(sql)) {
            ps.setInt(1, storeId);
            ps.setString(2, storeName);
            ps.executeUpdate();
        }
    }

    private boolean checkDate(String orderDateStr) throws ParseException{
        // Splitting the date string to remove the time part
        String[] parts = orderDateStr.split(" ");
        if (parts.length < 1) {
            System.out.println("Invalid date format: " + orderDateStr);
            return false;
        }
        if (parts[0].split("/")[2].length() >= 3) {
            return false;
        }else{
            return true;
        }
    }

    private java.sql.Date cleanDate(String orderDateStr){
        try {
            // Splitting the date string to remove the time part
            String[] parts = orderDateStr.split(" ");
            if (parts.length < 1) {
                System.out.println("Invalid date format: " + orderDateStr);
                return null;
            }
            // Parsing the date into java.util.Date
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy");
            if (parts[0].split("/")[2].length() >= 3) {
                return null;
            }else{
                java.util.Date utilDate = format.parse(parts[0]);
            // Convert java.util.Date to java.sql.Date
                java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
                return sqlDate;
            }
        }catch (ParseException e){
            System.out.println("Error parsing order date: " + orderDateStr);
            return null;
        }
    }

    private void insertTimeDataFromOrderDate(String orderDateStr) {
        try {
            // Splitting the date string to remove the time part
            String[] parts = orderDateStr.split(" ");
            if (parts.length < 1) {
                System.out.println("Invalid date format: " + orderDateStr);
                return;
            }
            
            // Parsing the date into java.util.Date
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy");
            java.util.Date utilDate = format.parse(parts[0]);
    
            // Convert java.util.Date to java.sql.Date
            java.sql.Date sqlDate = cleanDate(orderDateStr);        //makes sure to remove any dates that are not in the 2000s
    
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(utilDate);
    
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            int month = calendar.get(Calendar.MONTH) + 1; // Calendar.MONTH is zero-based
            int year = calendar.get(Calendar.YEAR);
    
            // Handling anomalies in year
            if (year < 100) { // Assuming all dates should be in the 2000s
                year += 2000;
            } else if (year < 2000) {
                System.out.println("Anomoloy year: " + year);
                return;
            }
    
            int quarter = (month - 1) / 3 + 1;
    
            // Insert into TIME_DIM

            insertIntoTimeDim(sqlDate, day, month, quarter, year);
        } catch (ParseException e) {
            System.out.println("Error parsing order date: " + orderDateStr);
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }
    
    
    private void insertIntoTimeDim(Date date, int day, int month, int quarter, int year) throws SQLException {
        String sql = "INSERT INTO TIME_DIM (Date_, Day, Month, Quarter, Year) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connDW.prepareStatement(sql)) {
            ps.setDate(1, date);
            ps.setInt(2, day);
            ps.setInt(3, month);
            ps.setInt(4, quarter);
            ps.setInt(5, year);
            ps.executeUpdate();
        }
    }

// Check if a supplier ID is already in use
private boolean isSupplierIdInUse(int supplierID) throws SQLException {
    // Query the database to check if the supplier ID is already in use
    String sql = "SELECT COUNT(*) FROM SUPPLIER_DIM WHERE supplierID = ?";
    try (PreparedStatement ps = connDW.prepareStatement(sql)) {
        ps.setInt(1, supplierID);
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                int count = rs.getInt(1);
                return count > 0; // Return true if the ID is in use, false otherwise
            }
        }
    }
    return false; // Return false if there was an error or the ID is not in use
}

// Get a new unique supplier ID
private int getNewSupplierId() throws SQLException {
    // Query the database to find the highest existing supplier ID and increment it
    String sql = "SELECT MAX(supplierID) FROM master_data";
    try (PreparedStatement ps = connMD.prepareStatement(sql)) {
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                int maxSupplierID = rs.getInt(1);
                // Increment the maximum supplier ID by 1 to get a new unique ID
                return maxSupplierID + 1;
            }
        }
    }
    // If there are no existing supplier IDs, start with 102 (greater than 101)
    counter++;
    return 52+counter;
}
                       

}
