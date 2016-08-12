package com.fs714.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.CYCTYPE;
import cn.com.wind.td.tdb.Code;
import cn.com.wind.td.tdb.KLine;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TdbTest {
    private Tdb tdb;

    @BeforeTest
    public void initTdbClient() {
        String ip = "114.80.154.34";
        int port = 6271;
        String username = "TD1132104002";
        String password = "44132392";
        tdb = new Tdb(ip, port, username, password);
    }

    @Test
    public void getCodesTest() {
        String market = "SZ-2-0";
        Code[] codes = tdb.getCodes(market);
        Tdb.printCodes(codes);
    }

    @Test
    public void getKlinesTest() {
        String code = "000001.SZ";
        String market = "SZ-2-0";
        int cyctype = CYCTYPE.CYC_DAY;
        int startDate = 20160801;
        int endDate = 20160812;

        KLine[] kLines = tdb.getKLines(code, market, cyctype, startDate, endDate);
        Tdb.printKlines(kLines);
    }
}