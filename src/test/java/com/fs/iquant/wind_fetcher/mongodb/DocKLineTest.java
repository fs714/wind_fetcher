package com.fs.iquant.wind_fetcher.mongodb;

import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import com.fs.iquant.wind_fetcher.tdb.enums.RefillFlag;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.set;

public class DocKLineTest {
    private static Logger logger = Logger.getLogger(DocKLineTest.class.getCanonicalName());
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> col;

    @BeforeClass
    public void initMongo() {
        String ip = "127.0.0.1";
        int port = 27017;
        client = new MongoClient(ip, port);
        db = client.getDatabase("mytest");
        col = db.getCollection("testKLine");
        col.drop();
    }

    @AfterClass(enabled = false)
    public void closeMongo() {
        db.drop();
        client.close();
    }

    @Test(enabled = false)
    public void insertTest() throws ParseException {
        logger.info("Start insertTest");
        DocKLine kl1 = new DocKLine("600000.SH", "600000", "PuFaYinHang", "SH-2-0", RefillFlag.REFILL_BACKWARD.getFlag(),
                CycType.CYC_MINUTE.getFlag(), 1, 20160818, 94500000, 200, 400, 100, 300, 1000, 2000, 10);
        DocKLine kl2 = new DocKLine("600000.SH", "600000", "PuFaYinHang", "SH-2-0", RefillFlag.REFILL_BACKWARD.getFlag(),
                CycType.CYC_MINUTE.getFlag(), 1, 20160818, 104500000, 2000, 4000, 1000, 3000, 10000, 20000, 100);
        DocKLine kl3 = new DocKLine("600000.SH", "600000", "PuFaYinHang", "SH-2-0", RefillFlag.REFILL_BACKWARD.getFlag(),
                CycType.CYC_MINUTE.getFlag(), 1, 20160818, 144500000, 20000, 40000, 10000, 30000, 100000, 200000, 1000);
        logger.info(kl1.getDate());
        logger.info(kl2.getDate());
        logger.info(kl3.getDate());
        col.insertOne(kl1.getDocument());
        col.insertOne(kl2.getDocument());
        col.insertOne(kl3.getDocument());
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void findTest() throws ParseException {
        logger.info("Start findTest");
        DocKLine kl;

        logger.info("------ Find one by first() ------");
        kl = new DocKLine(col.find().first());
        logger.info(kl.toString());

        logger.info("------ Find one with sort() ascending ------");
        kl = new DocKLine(col.find().sort(ascending("date")).first());
        logger.info(kl.toString());

        logger.info("------ Find one with sort() descending ------");
        kl = new DocKLine(col.find().sort(descending("date")).first());
        logger.info(kl.toString());

        logger.info("------ Find all by Cursor ------");
        MongoCursor<Document> cursor = col.find().iterator();
        try {
            while (cursor.hasNext()) {
                kl = new DocKLine(cursor.next());
                logger.info(kl.toString());
            }
        } finally {
            cursor.close();
        }

        logger.info("------ Find one with filter ------");
        kl = new DocKLine(col.find(eq("windCode", "600000.SH")).first());
        logger.info(kl.toString());

        logger.info("------ Find a set by filter and Cursor ------");
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date date = format.parse("20160818103000000");
        cursor = col.find(gt("date", date)).iterator();
        try {
            while (cursor.hasNext()) {
                kl = new DocKLine(cursor.next());
                logger.info(kl.toString());
            }
        } finally {
            cursor.close();
        }

        logger.info("------ Find null with filter ------");
        Document doc = col.find(eq("windCode", "900000.SH")).first();
        if (doc == null) {
            logger.info("Find Nothing");
        }
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void updateTest() {
        logger.info("Start updateTest");
        DocKLine kl;
        kl = new DocKLine(col.find(eq("windCode", "600000.SH")).first());
        kl.setName("PuFaBank");
        col.updateOne(eq("_id", kl.get_id()), set("name", kl.getName()));
        logger.info(col.find(eq("_id", kl.get_id())).first().get("name"));

        kl = new DocKLine(col.find(eq("windCode", "600000.SH")).first());
        col.updateOne(eq("_id", kl.get_id()), set("name", "PuFaYinHang"));
        kl.setName("PuFaYinHang");
        logger.info(col.find(eq("_id", kl.get_id())).first().get("name"));
    }

    @Test(enabled = false, dependsOnMethods = "insertTest")
    public void replaceTest() throws ParseException {
        logger.info("Start replaceTest");
        DocKLine kl;
        kl = new DocKLine(col.find(eq("windCode", "600000.SH")).first());
        kl.setName("PuFaBank");
        col.replaceOne(eq("_id", kl.get_id()), kl.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        kl = new DocKLine(col.find(eq("windCode", "600000.SH")).first());
        kl.setName("PuFaYinHang");
        col.replaceOne(eq("_id", kl.get_id()), kl.getDocument());
        logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());

        DocKLine klReplace = new DocKLine("600000.SH", "600000", "PuFaYinHang", "SH-2-0", RefillFlag.REFILL_BACKWARD.getFlag(),
                CycType.CYC_MINUTE.getFlag(), 1, 20160818, 94500000, 200, 400, 100, 300, 1000, 2000, 10);
        try {
            col.replaceOne(eq("windCode", "600000.SH"), klReplace.getDocument());
        } catch (Exception e) {
            logger.info(e.getMessage());
            logger.info(col.find(eq("windCode", "600000.SH")).first().toJson());
        }
    }
}
