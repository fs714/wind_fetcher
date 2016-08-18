package com.fs.iquant.wind_fetcher.mongodb;

import com.fs.iquant.wind_fetcher.tdb.enums.CodeType;
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

public class DocCodeTest {
    private static Logger logger = Logger.getLogger(DocCodeTest.class.getCanonicalName());
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> col;

    @BeforeClass
    public void initMongo() {
        String ip = "127.0.0.1";
        int port = 27017;
        client = new MongoClient(ip, port);
        db = client.getDatabase("mytest");
        col = db.getCollection("testDocCode");
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
        DocCode code1 = new DocCode("600000.SH", "600000", "SH-2-0", "PuFaYinHang", CodeType.ID_BT_SHARES_A.getIndex());
        DocCode code2 = new DocCode("600004.SH", "600004", "SH-2-0", "BaiYunJiChang", CodeType.ID_BT_SHARES_A.getIndex());
        DocCode code3 = new DocCode("600005.SH", "600005", "SH-2-0", "WuGangGuFen", CodeType.ID_BT_SHARES_A.getIndex());
        col.insertOne(code1.getDocument());
        col.insertOne(code2.getDocument());
        col.insertOne(code3.getDocument());
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void findTest() {
        logger.info("Start findTest");
        DocCode code;

        logger.info("------ Find one by first() ------");
        code = new DocCode(col.find().first());
        logger.info(code.toString());

        logger.info("------ Find one with sort() ascending ------");
        code = new DocCode(col.find().sort(ascending("code")).first());
        logger.info(code.toString());

        logger.info("------ Find one with sort() descending ------");
        code = new DocCode(col.find().sort(descending("code")).first());
        logger.info(code.toString());

        logger.info("------ Find all by Cursor ------");
        MongoCursor<Document> cursor = col.find().iterator();
        try {
            while (cursor.hasNext()) {
                code = new DocCode(cursor.next());
                logger.info(code.toString());
            }
        } finally {
            cursor.close();
        }

        logger.info("------ Find one with filter ------");
        code = new DocCode(col.find(eq("windCode", "600000.SH")).first());
        logger.info(code.toString());

        logger.info("------ Find a set by filter and Cursor ------");
        cursor = col.find(gt("type", 15)).iterator();
        try {
            while (cursor.hasNext()) {
                code = new DocCode(cursor.next());
                logger.info(code.toString());
            }
        } finally {
            cursor.close();
        }
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void updateTest() {
        logger.info("Start updateTest");
        DocCode code;
        code = new DocCode(col.find(eq("windCode", "600000.SH")).first());
        code.setName("PuFaBank");
        col.updateOne(eq("_id", code.get_id()), set("name", code.getName()));
        logger.info(col.find(eq("_id", code.get_id())).first().get("name"));

        code = new DocCode(col.find(eq("windCode", "600000.SH")).first());
        col.updateOne(eq("_id", code.get_id()), set("name", "PuFaYinHang"));
        code.setName("PuFaYinHang");
        logger.info(col.find(eq("_id", code.get_id())).first().get("name"));
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void replaceTest() {
        logger.info("Start replaceTest");
        DocCode code;
        code = new DocCode(col.find(eq("windCode", "600000.SH")).first());
        code.setName("PuFaBank");
        col.replaceOne(eq("_id", code.get_id()), code.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        code = new DocCode(col.find(eq("windCode", "600000.SH")).first());
        code.setName("PuFaYinHang");
        col.replaceOne(eq("_id", code.get_id()), code.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        DocCode codeReplace = new DocCode("600000.SH", "600000", "SH-2-0", "PuFa", CodeType.ID_BT_SHARES_A.getIndex());
        try {
            col.replaceOne(eq("windCode", "600000.SH"), codeReplace.getDocument());
        } catch (Exception e) {
            logger.info(e.getMessage());
            logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());
        }
    }
}
