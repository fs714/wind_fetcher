package com.fs.iquant.wind_fetcher.tdb;

import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import com.fs.iquant.wind_fetcher.mongodb.DocCode;
import com.fs.iquant.wind_fetcher.mongodb.DocCodeTest;
import com.fs.iquant.wind_fetcher.tdb.enums.CodeType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CodeRecorderTest {
    private static Logger logger = Logger.getLogger(DocCodeTest.class.getCanonicalName());
    private Tdb tdb;
    private MongoClient client;
    private MongoDatabase db;
    private CodeRecorder codeRec;

    @BeforeClass
    public void initMongo() throws TdbGetDataException {
        String tdbIp = "114.80.154.34";
        int tdbPort = 6271;
        String username = "TD1132104002";
        String password = "44132392";
        tdb = new Tdb(tdbIp, tdbPort, username, password);

        String mongoIp = "127.0.0.1";
        int mongoPort = 27017;
        client = new MongoClient(mongoIp, mongoPort);
        db = client.getDatabase("mytest");

        codeRec = new CodeRecorder(tdb, db);
        codeRec.drop();
    }

    @AfterClass(enabled = false)
    public void closeMongo() {
        db.drop();
        client.close();
        tdb.getClient().close();
    }

    @Test(enabled = true)
    public void saveTest() {
        logger.info("Start saveTest");
        codeRec.save();
        logger.info("Collection Items Num: " + codeRec.getCodeCol().getCol().count());
        logger.info(new DocCode(codeRec.getCodeCol().getCol().find().first()).toString());
    }

    @Test(enabled = true, dependsOnMethods = "saveTest")
    public void dropTest() {
        logger.info("Start dropTest");
        codeRec.drop();
        logger.info("Collection Items Num: " + codeRec.getCodeCol().getCol().count());
    }
}
