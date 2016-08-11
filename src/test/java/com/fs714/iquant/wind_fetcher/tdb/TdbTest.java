package com.fs714.iquant.wind_fetcher.tdb;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TdbTest {
    private static final String TEST_MARKET = "SZ-2-0";
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
    public void getCodeTableTest() {
        tdb.getCodeTable(TEST_MARKET);
    }
}