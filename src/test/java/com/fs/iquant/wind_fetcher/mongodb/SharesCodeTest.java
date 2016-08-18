package com.fs.iquant.wind_fetcher.mongodb;

import com.fs.iquant.wind_fetcher.tdb.CodeType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.set;

public class SharesCodeTest {
    private static Logger logger = Logger.getLogger(SharesCodeTest.class.getCanonicalName());
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> col;

    @BeforeClass
    public void initMongo() {
        String ip = "127.0.0.1";
        int port = 27017;
        client = new MongoClient(ip, port);
        db = client.getDatabase("mytest");
        col = db.getCollection("testSharesCode");
        col.drop();
    }

    @AfterClass(enabled = false)
    public void closeMongo() {
        db.drop();
        client.close();
    }

    @Test(enabled = false)
    public void insertTest() {
        logger.info("Start insertTest");
        SharesCode sc1 = new SharesCode("600000.SH", "600000", "SH-2-0", "PuFaYinHang", CodeType.ID_BT_SHARES_A.getIndex());
        SharesCode sc2 = new SharesCode("600004.SH", "600004", "SH-2-0", "BaiYunJiChang", CodeType.ID_BT_SHARES_A.getIndex());
        SharesCode sc3 = new SharesCode("600005.SH", "600005", "SH-2-0", "WuGangGuFen", CodeType.ID_BT_SHARES_A.getIndex());
        col.insertOne(sc1.getDocument());
        col.insertOne(sc2.getDocument());
        col.insertOne(sc3.getDocument());
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void findTest() {
        logger.info("Start findTest");
        SharesCode sc;

        logger.info("------ Find one by first() ------");
        sc = new SharesCode(col.find().first());
        logger.info(sc.toString());

        logger.info("------ Find one with sort() ascending ------");
        sc = new SharesCode(col.find().sort(ascending("code")).first());
        logger.info(sc.toString());

        logger.info("------ Find one with sort() descending ------");
        sc = new SharesCode(col.find().sort(descending("code")).first());
        logger.info(sc.toString());

        logger.info("------ Find all by Cursor ------");
        MongoCursor<Document> cursor = col.find().iterator();
        try {
            while (cursor.hasNext()) {
                sc = new SharesCode(cursor.next());
                logger.info(sc.toString());
            }
        } finally {
            cursor.close();
        }

        logger.info("------ Find one with filter ------");
        sc = new SharesCode(col.find(eq("windCode", "600000.SH")).first());
        logger.info(sc.toString());

        logger.info("------ Find a set by filter and Cursor ------");
        cursor = col.find(gt("type", 15)).iterator();
        try {
            while (cursor.hasNext()) {
                sc = new SharesCode(cursor.next());
                logger.info(sc.toString());
            }
        } finally {
            cursor.close();
        }
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void updateTest() {
        logger.info("Start updateTest");
        SharesCode sc;
        sc = new SharesCode(col.find(eq("windCode", "600000.SH")).first());
        sc.setName("PuFaBank");
        col.updateOne(eq("_id", sc.get_id()), set("name", sc.getName()));
        logger.info(col.find(eq("_id", sc.get_id())).first().get("name"));

        sc = new SharesCode(col.find(eq("windCode", "600000.SH")).first());
        col.updateOne(eq("_id", sc.get_id()), set("name", "PuFaYinHang"));
        sc.setName("PuFaYinHang");
        logger.info(col.find(eq("_id", sc.get_id())).first().get("name"));
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void replaceTest() {
        logger.info("Start replaceTest");
        SharesCode sc;
        sc = new SharesCode(col.find(eq("windCode", "600000.SH")).first());
        sc.setName("PuFaBank");
        col.replaceOne(eq("_id", sc.get_id()), sc.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        sc = new SharesCode(col.find(eq("windCode", "600000.SH")).first());
        sc.setName("PuFaYinHang");
        col.replaceOne(eq("_id", sc.get_id()), sc.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        SharesCode scReplace = new SharesCode("600000.SH", "600000", "SH-2-0", "PuFa", CodeType.ID_BT_SHARES_A.getIndex());
        try {
            col.replaceOne(eq("windCode", "600000.SH"), scReplace.getDocument());
        } catch (Exception e) {
            logger.info(e.getMessage());
            logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());
        }
    }
}
