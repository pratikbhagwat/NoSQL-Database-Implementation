package NoSqlDatabaseImplementation;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Accumulators;
import org.bson.Document;

import javax.print.Doc;

import static com.mongodb.client.model.Accumulators.max;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;


public class NoSqlDatabase implements DBFunctionalities {

    private MongoClient mongoClient;
    private String mongoDbName;
    public NoSqlDatabase(MongoClient mongoClient,String mongoDbName){
        this.mongoClient = mongoClient;
        this.mongoDbName = mongoDbName;

    }
    @Override
    public void createAccount(String username, String password, String firstName, String lastName) throws MongoException {
        Document accountDocument = new Document();
        accountDocument.put("_id",new Document("username",username));
        accountDocument.put("Password",password);
        accountDocument.put("FirstName",firstName);
        accountDocument.put("LastName",lastName);
        this.mongoClient.getDatabase(this.mongoDbName).getCollection("Users").insertOne(accountDocument);
    }

    @Override
    public void submitOrder(String date, String username, String password, Map<Integer, Integer> listOfProductsAndQuantities) throws MongoException {
        if (authenticateUser(username,password)){
            updateStockAndInsertOrders(date,username,listOfProductsAndQuantities);
        }else {
            throw new MongoException("user not authenticate");
        }
    }

    /**
     * update the stocks and insert the orders in order table
     * @param date
     * @param username
     * @param listOfProductsAndQuantities
     * @throws MongoException
     */
    private void updateStockAndInsertOrders(String date, String username, Map<Integer, Integer> listOfProductsAndQuantities) throws MongoException {
        if( validateOrder(listOfProductsAndQuantities)){
            updateStockLevelInBulk(listOfProductsAndQuantities);
            updateTheOrderTable(date,username,listOfProductsAndQuantities);
        }else {
            throw new MongoException("order not valid due to insufficient items in inventory");
        }
    }

    /**
     * inserts the entries in order table
     * @param date
     * @param username
     * @param listOfProductsAndQuantities
     */
    private void updateTheOrderTable(String date, String username, Map<Integer, Integer> listOfProductsAndQuantities) {
        Document orderDocument = new Document();
        orderDocument.put("date",date);
        orderDocument.put("username",username);
        Document itemsInOrderDocument = new Document();
        for (Map.Entry<Integer,Integer> productQuantityEntry:listOfProductsAndQuantities.entrySet()){
            itemsInOrderDocument.put(""+productQuantityEntry.getKey(),""+productQuantityEntry.getValue());
        }
        orderDocument.put("orderDetails",itemsInOrderDocument);
        this.mongoClient.getDatabase(this.mongoDbName).getCollection("Orders").insertOne(orderDocument);
    }

    /**
     * Updates the stock levels of all the products in the order list
     * @param listOfProductsAndQuantities
     * @throws MongoException
     */
    private void updateStockLevelInBulk(Map<Integer, Integer> listOfProductsAndQuantities) throws MongoException {
        for (Map.Entry<Integer,Integer> productQuantityEntry:listOfProductsAndQuantities.entrySet()){
            updateStockLevel(productQuantityEntry.getKey(),-productQuantityEntry.getValue());// updating the table with negative value
        }
    }

    /**
     *
     * @param listOfProductsAndQuantities
     * @return whether the order specified can be fulfilled.
     */
    private boolean validateOrder(Map<Integer, Integer> listOfProductsAndQuantities) {
        AggregateIterable<Document> result = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Products").aggregate(Arrays.asList(match(in("_id", Arrays.asList(listOfProductsAndQuantities.keySet().toArray())))));
        for (Document resultDocument : result) {
            if ( resultDocument.getInteger("stock") < listOfProductsAndQuantities.get(resultDocument.getInteger("_id"))){
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param username
     * @param password
     * @return whether the user is authentic or not.
     */
    private boolean authenticateUser(String username, String password) {
        AggregateIterable<Document> result = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Users").aggregate(Arrays.asList(match(eq("_id", eq("username", username)))));
        for (Document documentResult:result){
            if (documentResult.getString("Password").equals(password)){
                return true;
            }
        }
        return false;
    }

    @Override
    public void postReview(String username, String password, int productID, Rating rating, String reviewText) throws MongoException {
        if (authenticateUser(username,password)){
            if (authorizeUser(username,productID)){
                Document reviewDocument = new Document();
                Document idDocument = new Document();
                idDocument.put("username",username);
                idDocument.put("productId",productID);
                reviewDocument.put("_id",idDocument);
                reviewDocument.put("reviewText",reviewText);
                reviewDocument.put("rating",rating.ordinal()+1);
                this.mongoClient.getDatabase(this.mongoDbName).getCollection("Reviews").insertOne(reviewDocument);
            }else {
                throw new MongoException("User not authorized");
            }
        }else {
            throw new MongoException("User not authentic");
        }
    }

    /**
     *
     * @param username
     * @param productID
     * @return whether the user is authorized to give review for the product.
     */
    public boolean authorizeUser(String username, int productID) {
        AggregateIterable<Document> result = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Orders").aggregate(Arrays.asList(match(and(eq("username", "user131"), exists("orderDetails."+productID, true)))));

        for (Document resultDocument : result){ // if we enter this loop means user is authorized
            return true;
        }
        return false;
    }

    @Override
    public void addProduct(String name, String description, double price, int initialStock) throws MongoException {
        int maxProductId = getMaxProductIdInTable();
        Document productDocument = new Document();
        productDocument.put("_id",maxProductId+1);
        productDocument.put("name",name);
        productDocument.put("description",description);
        productDocument.put("price",price);
        productDocument.put("stock",initialStock);
        this.mongoClient.getDatabase(this.mongoDbName).getCollection("Products").insertOne(productDocument);
    }

    /**
     *
     * @return max product id present in the table
     */
    private int getMaxProductIdInTable() {
        AggregateIterable<Document> result = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Products").aggregate(Arrays.asList(group("", max("max", "$_id"))));
        for (Document resultDocument:result){

            return resultDocument.getInteger("max");

        }
        return 0;
    }

    @Override
    public void updateStockLevel(int productID, int itemCountToAdd) throws MongoException {
        this.mongoClient.getDatabase(this.mongoDbName).getCollection("Products").updateOne(
                new Document("_id",productID),
                new Document("$inc",new Document("stock",itemCountToAdd)));
    }



    @Override
    public void getProductAndReviews(int productID) throws MongoException {
        AggregateIterable<Document> productData = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Products").aggregate(Arrays.asList(match(eq("_id", productID))));
        AggregateIterable<Document> reviewsData = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Reviews").aggregate(Arrays.asList(match(eq("_id.productId", productID))));

        /**
         * print product data
         */
//        for (Document productDataDocument:productData){
//            System.out.println(productDataDocument.toString());
//        }

        /**
         * print the review data
         */
//        for (Document reviewDataDocument : reviewsData){
//            System.out.println(reviewDataDocument.toString());
//        }

    }

    @Override
    public void getAverageUserRating(String username) throws MongoException {
        AggregateIterable<Document> result = this.mongoClient.getDatabase(this.mongoDbName).getCollection("Reviews").aggregate(Arrays.asList(match(eq("_id.username", "user" + username)), group("$_id.username", Accumulators.avg("average", "$rating"))));

        for (Document resultDocument:result){
            double averageRating = resultDocument.getDouble("average");
//            System.out.println(averageRating); // uncomment this to see the rating.
            break;
        }
    }
}
