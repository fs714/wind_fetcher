package com.fs.iquant.wind_fetcher.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class ColBase {
    private MongoDatabase db;
    private MongoCollection<Document> col;
    private String name;

    public ColBase(MongoDatabase db, String name) {
        this.db = db;
        this.name = name;
        this.col = db.getCollection(name);
    }

    public MongoDatabase getDb() {
        return db;
    }

    public MongoCollection<Document> getCol() {
        return col;
    }

    public String getName() {
        return name;
    }
}
