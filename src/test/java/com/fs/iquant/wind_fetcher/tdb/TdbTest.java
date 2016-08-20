package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import cn.com.wind.td.tdb.KLine;
import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import com.fs.iquant.wind_fetcher.tdb.enums.CodeType;
import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TdbTest {
    private static final String EXAMPLE_CODE_ONE = "000001.SZ";
    private static Logger logger = Logger.getLogger(TdbTest.class.getCanonicalName());
    private Tdb tdb;

    @BeforeClass
    public void initTdbClient() throws TdbGetDataException {
        String ip = "114.80.154.34";
        int port = 6271;
        String username = "TD1132104002";
        String password = "44132392";
        tdb = new Tdb(ip, port, username, password);
        logger.info("Connection to tdb service");
    }

    @Test(enabled = false)
    public void getCodesTest() throws TdbGetDataException {
        logger.info("------------ SH ------------");
        for (CodeType type : CodeType.values()) {
            Code[] codes = tdb.getCodes(Tdb.MARKET_SH_SHARES, type.getIndex());
            logger.info("-----" + type.toString() + "(" + type.getIndex() + "): -----");
            Tdb.printCodes(codes);
        }
        logger.info("------------ SZ ------------");
        for (CodeType type : CodeType.values()) {
            Code[] codes = tdb.getCodes(Tdb.MARKET_SZ_SHARES, type.getIndex());
            logger.info("-----" + type.toString() + "(" + type.getIndex() + "): -----");
            Tdb.printCodes(codes);
        }
    }

    @Test(enabled = false)
    public void getCodeInfoTest() throws TdbGetDataException {
        Code code = tdb.getCodeInfo(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES);
        Tdb.printCode(code);
    }

    @Test(enabled = false)
    public void getShSharesATest() {
        Code[] codes = tdb.getShSharesA();
        logger.info("------ Total Number getShSharesA: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzSharesATest() {
        Code[] codes = tdb.getSzSharesA();
        logger.info("------ Total Number getSzSharesA: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzSharesSTest() {
        Code[] codes = tdb.getSzSharesS();
        logger.info("------ Total Number getSzSharesS: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzSharesGTest() {
        Code[] codes = tdb.getSzSharesG();
        logger.info("------ Total Number getSzSharesG: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzSharesAllTest() {
        Code[] codes = tdb.getSzSharesAll();
        logger.info("------ Total Number getSzSharesAll: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getShIndexTest() {
        Code[] codes = tdb.getShIndex();
        logger.info("------ Total Number getShIndex: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzIndexTest() {
        Code[] codes = tdb.getSzIndex();
        logger.info("------ Total Number getSzIndex: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getShSharesAndIndexTest() {
        Code[] codes = tdb.getShSharesAndIndex();
        logger.info("------ Total Number getShSharesAndIndex: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getSzSharesAndIndexTest() {
        Code[] codes = tdb.getSzSharesAndIndex();
        logger.info("------ Total Number getSzSharesAndIndex: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getAllSharesAndIndexTest() {
        Code[] codes = tdb.getAllSharesAndIndex();
        logger.info("------ Total Number getAllSharesAndIndex: " + codes.length + " ------");
        Tdb.printCodes(codes);
    }

    @Test(enabled = false)
    public void getKlinesTest() {
        KLine[] kLines;
        int cyctype = CycType.CYC_MINUTE.getFlag();
        int beginDate = 20130801;
        int endDate = 20130801;
        int beginTime = 143000000;
        int endTime = 0;

        kLines = tdb.getKLines(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES, cyctype, 1, beginDate, endDate, beginTime, endTime);
        Tdb.printKlines(kLines);
        kLines = tdb.getKLines(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES, cyctype, 2, beginDate, endDate, beginTime, endTime);
        Tdb.printKlines(kLines);
        kLines = tdb.getKLines(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES, cyctype, 3, beginDate, endDate, beginTime, endTime);
        Tdb.printKlines(kLines);
        kLines = tdb.getKLines(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES, cyctype, 4, beginDate, endDate, beginTime, endTime);
        Tdb.printKlines(kLines);

        cyctype = CycType.CYC_DAY.getFlag();
        beginDate = 19900101;
        endDate = 0;

        kLines = tdb.getKLines(EXAMPLE_CODE_ONE, Tdb.MARKET_SZ_SHARES, cyctype, 1, beginDate, endDate);
        Tdb.printKlines(kLines);
    }

    @Test(enabled = false)
    public void getMultiKlinesTest() {
        String[] codeList = new String[]{"000731.SZ", "000732.SZ", "000733.SZ", "000735.SZ", "000736.SZ",};
        String market = "SZ-2-0";
        int cyctype = CycType.CYC_DAY.getFlag();
        int cycdef = 1;
        int beginDate = 20160801;
        int endDate = 20160812;
        int beginTime = 0;
        int endTime = 0;

        SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        Date startTimeStamp = new Date(System.currentTimeMillis());
        String retStrFormatStartDate = sdFormatter.format(startTimeStamp);
        logger.info(retStrFormatStartDate);
        for (String code : codeList) {
            KLine[] kLines = tdb.getKLines(code, market, cyctype, cycdef, beginDate, endDate, beginTime, endTime);
            Tdb.printKlines(kLines);
        }
        Date endTimeStamp = new Date(System.currentTimeMillis());
        String retStrFormatEndDate = sdFormatter.format(endTimeStamp);
        logger.info(retStrFormatEndDate);
    }
}