package com.fs714.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import cn.com.wind.td.tdb.OPEN_SETTINGS;
import cn.com.wind.td.tdb.ResLogin;
import cn.com.wind.td.tdb.TDBClient;
import org.apache.log4j.Logger;

public class Tdb {
    private Logger logger;
    private TDBClient client;

    Tdb(String ip, int port, String username, String password) {
        logger = Logger.getLogger(Tdb.class.getCanonicalName());
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

    public void getCodeTable(String marker) {
        Code[] codes = client.getCodeTable(marker);

        if (codes == null) {
            logger.error("NetWork Error,getKline failed!");
            return;
        }

        for (Code code : codes) {
            logger.info("CODE: " + code.getWindCode() + ", " + code.getMarket() + ", " + code.getCode() + ", "
                    + code.getENName() + ", " + code.getCNName() + ", " + code.getType());
        }
    }
}
