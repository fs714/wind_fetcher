package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import com.fs.iquant.wind_fetcher.mongodb.DocKLine;
import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import com.fs.iquant.wind_fetcher.util.Util;
import com.fs.iquant.wind_fetcher.util.WindDateTime;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.mongodb.client.model.Filters.*;

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
        calendar.add(Calendar.DAY_OF_MONTH, -30);
        Date begin = calendar.getTime();

        long t1 = System.currentTimeMillis();

        klineRec.saveNoOverwrite(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t2 = System.currentTimeMillis();

        klineRec.drop();
        klineRec.saveNoOverwriteNoCheck(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t3 = System.currentTimeMillis();

        klineRec.saveNoOverwrite(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t4 = System.currentTimeMillis();

        logger.info("t2 - t1 = " + (t2 - t1)/1000);
        logger.info("t3 - t2 = " + (t3 - t2)/1000);
        logger.info("t4 - t3 = " + (t4 - t3)/1000);
    }

    @Test(enabled = false)
    public void saveNewTest() throws ParseException {
        logger.info("Start saveNewTest");
        int cycType = CycType.CYC_MINUTE.getFlag();

        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), 1, 6, 0, 0);
        c.add(Calendar.YEAR, -3);
        Date begin = c.getTime();
        Date end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30, Calendar.SECOND, -1);

        long t1 = System.currentTimeMillis();

        klineRec.saveNewNoOverwrite(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t2 = System.currentTimeMillis();

        klineRec.saveNewNoOverwrite(cycType, begin, end);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t3 = System.currentTimeMillis();

        logger.info("t2 - t1 = " + (t2 - t1)/1000);
        logger.info("t3 - t2 = " + (t3 - t2)/1000);
    }

    @Test(enabled = false)
    public void saveNewHistoryTest() throws ParseException {
        logger.info("Start saveNewHistoryTest");
        int cycType = CycType.CYC_MINUTE.getFlag();

        long t1 = System.currentTimeMillis();

        klineRec.saveNewHistoryNoOverwrite(cycType);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t2 = System.currentTimeMillis();

        klineRec.saveNewHistoryNoOverwrite(cycType);
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t3 = System.currentTimeMillis();

        logger.info("t2 - t1 = " + (t2 - t1)/1000);
        logger.info("t3 - t2 = " + (t3 - t2)/1000);
    }

    @Test(enabled = false)
    public void saveNewAllTest() throws ParseException {
        logger.info("Start saveNewAllTest");
        int cycType = CycType.CYC_MINUTE.getFlag();

        long t1 = System.currentTimeMillis();

        klineRec.saveNewAllNoOverwrite();
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t2 = System.currentTimeMillis();

        klineRec.saveNewAllNoOverwrite();
        logger.info("Collection Items Num: " + klineRec.getKlineCol().getCol().count());
        long t3 = System.currentTimeMillis();

        logger.info("t2 - t1 = " + (t2 - t1)/1000);
        logger.info("t3 - t2 = " + (t3 - t2)/1000);
    }

    @Test(enabled = false)
    public void dateIntervalTest() throws ParseException {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), 1, 6, 0, 0);
        c.add(Calendar.YEAR, -3);

        Date begin = c.getTime();
        Date end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30, Calendar.SECOND, -1);
        while (begin.before(new Date())) {
            logger.info("--------------");
            logger.info(begin);
            logger.info(end);
            begin = Util.dateAdd(end, Calendar.SECOND, 1);
            end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30, Calendar.SECOND, -1);
        }
    }
}
