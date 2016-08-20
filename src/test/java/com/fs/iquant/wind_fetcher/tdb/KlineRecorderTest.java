package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import com.fs.iquant.wind_fetcher.mongodb.DocKLine;
import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import com.fs.iquant.wind_fetcher.util.WindDateTime;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

public class KlineRecorderTest {
    private static Logger logger = Logger.getLogger(KlineRecorderTest.class.getCanonicalName());
    private Tdb tdb;
    private MongoClient client;
    private MongoDatabase db;
    private KlineRecorder klineRec;

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

        Code code = tdb.getShSharesA()[0];

        klineRec = new KlineRecorder(tdb, db, code);
        klineRec.drop();
    }

    @AfterClass(enabled = false)
    public void closeMongo() {
        db.drop();
        client.close();
        tdb.getClient().close();
    }

    @Test(enabled = false)
    public void saveTest() throws ParseException {
        logger.info("Start saveTest");
        int cycType = CycType.CYC_MINUTE.getFlag();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        Date end = calendar.getTime();
        calendar.add(Calendar.DAY_OF_MONTH, -40);
        Date begin = calendar.getTime();

        klineRec.saveNoOverwrite(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        logger.info(new DocKLine(klineRec.getKlineCol().getCol().find().first()).toString());
    }

    @Test(enabled = false)
    public void saveAllTest() throws ParseException {
        logger.info("Start saveAllTest");
        int cycType = CycType.CYC_MINUTE.getFlag();

        WindDateTime windDate = new WindDateTime(20130801, 0);
        Date begin = windDate.getDate();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(begin);
        calendar.add(Calendar.DAY_OF_MONTH, 30);
        Date end = calendar.getTime();
        while (begin.before(new Date())) {
            klineRec.saveNoOverwrite(cycType, begin, end);
            begin = end;
            calendar.setTime(begin);
            calendar.add(Calendar.DAY_OF_MONTH, 30);
            end = calendar.getTime();
        }
    }
}
