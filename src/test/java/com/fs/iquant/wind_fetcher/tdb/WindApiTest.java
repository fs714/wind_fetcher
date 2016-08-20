package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.KLine;
import cn.com.wind.td.tdb.OPEN_SETTINGS;
import cn.com.wind.td.tdb.ReqKLine;
import cn.com.wind.td.tdb.TDBClient;
import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import com.fs.iquant.wind_fetcher.tdb.enums.RefillFlag;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class WindApiTest {
    private static final String EXAMPLE_CODE_ONE = "000001.SZ";
    private static Logger logger = Logger.getLogger(WindApiTest.class.getCanonicalName());
    private TDBClient client;
    private ReqKLine reqKL;

    @BeforeClass
    public void initTdbClient() {
        String ip = "114.80.154.34";
        int port = 6271;
        String username = "TD1132104002";
        String password = "44132392";

        client = new TDBClient();

        OPEN_SETTINGS setting = new OPEN_SETTINGS();
        setting.setIP(ip);
        setting.setPort(Integer.toString(port));
        setting.setUser(username);
        setting.setPassword(password);
        setting.setRetryCount(10);
        setting.setRetryGap(10);
        setting.setTimeOutVal(20);
        client.open(setting);

        reqKL = new ReqKLine();
        reqKL.setCode(EXAMPLE_CODE_ONE);
        reqKL.setMarketKey(Tdb.MARKET_SZ_SHARES);
        reqKL.setCQFlag(RefillFlag.REFILL_BACKWARD.getFlag());
        reqKL.setCycType(CycType.CYC_MINUTE.getFlag());
        reqKL.setCycDef(1);
        reqKL.setAutoComplete(0);

        logger.info("Connection to tdb service");
    }

    @Test(enabled = false)
    public void getKlinesTest() {
        KLine[] klines;

        reqKL.setBeginDate(20160812);
        reqKL.setEndDate(20160812);
        reqKL.setBeginTime(143000000);
        reqKL.setEndTime(144000000);
        klines = client.getKLine(reqKL);
        logger.info("Trading Day: " + klines.length);
        logger.info(Integer.toString(klines[0].getDate()) + Integer.toString(klines[0].getTime()) + " - " +
                Integer.toString(klines[klines.length - 1].getDate()) +
                Integer.toString(klines[klines.length - 1].getTime()));

        reqKL.setBeginDate(20160811);
        reqKL.setEndDate(20160812);
        reqKL.setBeginTime(0);
        reqKL.setEndTime(0);
        klines = client.getKLine(reqKL);
        logger.info("Trading Day with zero time: " + klines.length);
        logger.info(Integer.toString(klines[0].getDate()) + Integer.toString(klines[0].getTime()) + " - " +
                Integer.toString(klines[klines.length - 1].getDate()) +
                Integer.toString(klines[klines.length - 1].getTime()));

        reqKL.setBeginDate(20160813);
        reqKL.setEndDate(20160813);
        reqKL.setBeginTime(143000000);
        reqKL.setEndTime(144000000);
        klines = client.getKLine(reqKL);
        logger.info("Not Trading Day: " + klines.length);

        reqKL.setBeginDate(20160812);
        reqKL.setEndDate(20160812);
        reqKL.setBeginTime(153000000);
        reqKL.setEndTime(154000000);
        klines = client.getKLine(reqKL);
        logger.info("Not Trading Time: " + klines.length);

        reqKL.setBeginDate(20160812);
        reqKL.setEndDate(20160711);
        reqKL.setBeginTime(0);
        reqKL.setEndTime(0);
        klines = client.getKLine(reqKL);
        logger.info("More than 30 days: " + klines.length);

        reqKL.setBeginDate(20130701);
        reqKL.setEndDate(20130701);
        reqKL.setBeginTime(143000000);
        reqKL.setEndTime(144000000);
        klines = client.getKLine(reqKL);
        logger.info("Out of account range: " + klines.length);

        reqKL.setBeginDate(20130701);
        reqKL.setEndDate(20130801);
        reqKL.setBeginTime(143000000);
        reqKL.setEndTime(144000000);
        klines = client.getKLine(reqKL);
        logger.info("Out and in of account range: " + klines.length);

        reqKL.setCode("000003.SZ");
        reqKL.setBeginDate(20160812);
        reqKL.setEndDate(20160812);
        reqKL.setBeginTime(143000000);
        reqKL.setEndTime(144000000);
        klines = client.getKLine(reqKL);
        logger.info("Delisted Share Code: " + klines.length);
    }
}
