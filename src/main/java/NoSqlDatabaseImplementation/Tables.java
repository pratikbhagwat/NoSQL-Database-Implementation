package NoSqlDatabaseImplementation;

import com.mongodb.MongoClient;

public class Tables {
    private MongoClient mongoClient;
    private String mongoDbName;
    public Tables(MongoClient mongoClient, String mongoDbName) {
        this.mongoClient=mongoClient;
        this.mongoDbName=mongoDbName;
    }

    public void createTables() {
        mongoClient.getDatabase(mongoDbName).createCollection("Orders");
        mongoClient.getDatabase(mongoDbName).createCollection("Products");
        mongoClient.getDatabase(mongoDbName).createCollection("Reviews");
        mongoClient.getDatabase(mongoDbName).createCollection("Users");
    }
}
