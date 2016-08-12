package com.fs714.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.*;
import org.apache.log4j.Logger;

public class Tdb {
    private static Logger logger = Logger.getLogger(Tdb.class.getCanonicalName());
    private TDBClient client;

    Tdb(String ip, int port, String username, String password) {
        client = new TDBClient();

        OPEN_SETTINGS setting = new OPEN_SETTINGS();
        setting.setIP(ip);
        setting.setPort(Integer.toString(port));
        setting.setUser(username);
        setting.setPassword(password);
        setting.setRetryCount(10);
        setting.setRetryGap(10);
        setting.setTimeOutVal(20);

        ResLogin res = client.open(setting);

        if (res == null) {
            logger.error("Can't connect to " + ip);
            System.exit(-1);
        } else {
            logger.info(res.getMarkets());

            int count = res.getMarkets();
            String[] market = res.getMarket();
            int[] dyndate = res.getDynDate();

            for (int i = 0; i < count; i++) {
                logger.info(market[i] + " " + dyndate[i]);
            }
        }
    }

    public Code[] getCodes(String marker) {
        Code[] codes = client.getCodeTable(marker);

        if (codes == null) {
            logger.error("NetWork Error, getCode failed!");
        }

        return codes;
    }

    public static void printCodes(Code[] codes) {
        for (Code code : codes) {
            logger.info("CODE: " + code.getWindCode() + ", " + code.getMarket() + ", " + code.getCode() + ", "
                    + code.getENName() + ", " + code.getCNName() + ", " + code.getType());
        }
    }

    public KLine[] getKLines(String code, String market, int cyctype, int startDate, int endDate) {
        ReqKLine reqKL = new ReqKLine();
        reqKL.setCode(code);
        reqKL.setMarketKey(market);
        reqKL.setCycType(cyctype);
        reqKL.setCycDef(1);
        reqKL.setBeginDate(startDate);
        reqKL.setEndDate(endDate);

        KLine[] klines = client.getKLine(reqKL);

        if (klines == null) {
            logger.error("NetWork Error, getKline failed!");
        }

        return klines;
    }

    public static void printKlines(KLine[] klines) {
        logger.info("Success to get "  + klines.length + " klines for " + klines[0].getCode());
        for (KLine k : klines ){
            logger.info(k.getWindCode() + ", " + k.getDate() + ", " + k.getTime() + ", " + k.getOpen() + ", "
                    + k.getHigh() + ", " + k.getLow() + ", " + k.getClose() + ", " + k.getVolume() + ", "
                    + k.getTurover() + ", " + k.getMatchItems() + ", " + k.getInterest());
        }
    }
}
