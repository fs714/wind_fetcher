package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import java.text.ParseException;

public class TdbMain {
    private static Logger logger = Logger.getLogger(Tdb.class.getCanonicalName());

    public static void main(String[] args) throws TdbGetDataException, ParseException {
        if (args.length != 2) {
            printHelp();
            System.exit(0);
        }

        String tdbIp = "114.80.154.34";
        int tdbPort = 6271;
        String username = "TD1132104002";
        String password = "44132392";
        Tdb tdb = new Tdb(tdbIp, tdbPort, username, password);

        String mongoIp = "127.0.0.1";
        int mongoPort = 27017;
        MongoClient client = new MongoClient(mongoIp, mongoPort);
        MongoDatabase db = client.getDatabase("wind");

        if (args[0].equals("save")) {
            if (args[1].equals("all")) {
                Code[] allCodes = tdb.getAllSharesAndIndex();
                long t1, t2;
                for (Code code : allCodes) {
                    t1 = System.currentTimeMillis();
                    logger.info("Start to save " + code.getWindCode() + " " + code.getCNName());
                    KlineRecorder klineRec = new KlineRecorder(tdb, db, code);
                    klineRec.saveNewAllNoOverwrite();
                    t2 = System.currentTimeMillis();
                    logger.info("Elapsed Time: " + (t2 - t1) / 1000 + "s");
                }
            } else if (args[1].equals("code")) {
                CodeRecorder codeRec = new CodeRecorder(tdb, db);
                codeRec.save();
            } else {
                Code code = findCode(tdb, args[1]);
                if (code == null) {
                    System.out.println("Invalid wind code: " + args[1]);
                } else {
                    long t1, t2;
                    t1 = System.currentTimeMillis();
                    logger.info("Start to save " + code.getWindCode() + " " + code.getCNName());
                    KlineRecorder klineRec = new KlineRecorder(tdb, db, code);
                    klineRec.saveNewAllNoOverwrite();
                    t2 = System.currentTimeMillis();
                    logger.info("Elapsed Time: " + (t2 - t1) / 1000 + "s");
                }
            }
        } else if (args[0].equals("drop")) {
            if (args[1].equals("all")) {
                Code[] allCodes = tdb.getAllSharesAndIndex();
                for (Code code : allCodes) {
                    KlineRecorder klineRec = new KlineRecorder(tdb, db, code);
                    klineRec.drop();
                    logger.info("All date for " + code.getWindCode() + " " + code.getCNName() + " has been droped");
                }
            } else if (args[1].equals("code")) {
                CodeRecorder codeRec = new CodeRecorder(tdb, db);
                codeRec.drop();
            } else {
                Code code = findCode(tdb, args[1]);
                if (code == null) {
                    System.out.println("Invalid wind code: " + args[1]);
                } else {
                    KlineRecorder klineRec = new KlineRecorder(tdb, db, code);
                    klineRec.drop();
                    logger.info("All date for " + code.getWindCode() + " " + code.getCNName() + " has been droped");
                }
            }
        } else {
            printHelp();
            System.exit(0);
        }
    }

    public static void printHelp() {
        String cmd = "java -cp wind_fetcher-1.0-SNAPSHOT-jar-with-dependencies.jar:../src/main/resources/tdbapi.jar com.fs.iquant.wind_fetcher.tdb.TdbMain";
        System.out.println("Usage:");
        System.out.println(cmd + " save 600000.SH");
        System.out.println(cmd + " drop 600000.SH");
        System.out.println(cmd + " save all");
        System.out.println(cmd + " drop all");
        System.out.println(cmd + " save code");
        System.out.println(cmd + " drop code");
    }

    public static Code findCode(Tdb tdb, String windCode) {
        Code findCode = null;
        Code[] allCodes = tdb.getAllSharesAndIndex();
        for (Code code : allCodes) {
            if (code.getWindCode().equals(windCode)) {
                findCode = code;
                break;
            }
        }
        return findCode;
    }
}
