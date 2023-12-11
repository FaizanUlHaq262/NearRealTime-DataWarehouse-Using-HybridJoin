import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.sql.*;

public class tuple {
	
	private String productID;
	private String customerId;
	private String Gender;
	private String customerName;
	private String orderID;
	private String orderDate;
	private int quantity;
	public tuple()
	{
		this.productID="";
		this.customerId="";
		this.Gender="";
		this.customerName="";
		this.orderID="";
		this.orderDate=null;
		this.quantity=0;

	}
	public tuple(ResultSet transactions) throws SQLException
	{
		
		this.productID = transactions.getString("ProductID");
		this.customerId = transactions.getString("CustomerID");
		this.Gender = transactions.getString("Gender");
		this.orderID = transactions.getString("Order ID");
		this.orderDate = transactions.getString("Order Date");
		this.quantity = transactions.getInt("Quantity Ordered");
        this.customerName = transactions.getString("CustomerName");
		
	}
public String getProductID() {
    return productID;
}

public String getCustomerID() {
    return customerId;
}

public String getGender() {
    return Gender;
}

public String getCustomerName() {
    return customerName;
}

public String getOrderID() {
    return orderID;
}

public String getOrderDate() {
    return orderDate;
}

public int getQuantity() {
    return quantity;
}
}