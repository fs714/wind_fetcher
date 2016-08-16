package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.*;
import com.fs.iquant.wind_fetcher.util.Util;
import com.fs.iquant.wind_fetcher.exceptions.TdbConnectionException;
import com.fs.iquant.wind_fetcher.exceptions.TdbGetDataException;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class Tdb {
    public static final String MARKET_SH_SHARES = "SH-2-0";
    public static final String MARKET_SZ_SHARES = "SZ-2-0";

    private static Logger logger = Logger.getLogger(Tdb.class.getCanonicalName());
    private TDBClient client;

    private Code[] shSharesA;
    private Code[] szSharesA;
    private Code[] szSharesS;
    private Code[] szSharesG;

    private Code[] szSharesAll;

    private Code[] shIndex;
    private Code[] szIndex;

    private Code[] shSharesAndIndex;
    private Code[] szSharesAndIndex;

    private Code[] allSharesAndIndex;

    Tdb(String ip, int port, String username, String password) throws TdbConnectionException, TdbGetDataException {
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
            throw new TdbConnectionException("Failed to connect to TDB Server " + ip + ":" + port);
        } else {
            logger.info("Connected to TDB Server " + ip + ":" + port);
            logger.info(res.getMarkets());

            int count = res.getMarkets();
            String[] market = res.getMarket();
            int[] dyndate = res.getDynDate();

            for (int i = 0; i < count; i++) {
                logger.info(market[i] + " " + dyndate[i]);
            }
        }

        initCodes();
    }

    public Code[] getCodes(String market) throws TdbGetDataException {
        Code[] codes = client.getCodeTable(market);

        if (codes == null) {
            throw new TdbGetDataException("Return null with getCodes");
        }

        return codes;
    }

    public Code[] getCodes(String market, int codeType) throws TdbGetDataException {
        Code[] allCodes = getCodes(market);

        ArrayList<Code> codes = new ArrayList<>();
        for (Code code : allCodes) {
            if (code.getType() == codeType) {
                codes.add(code);
            }
        }

        return codes.toArray(new Code[codes.size()]);
    }

    public Code getCodeInfo(String code, String market) throws TdbGetDataException {
        Code codeInfo = client.getCodeInfo(code, market);

        if (codeInfo == null) {
            throw new TdbGetDataException("Return null with getCodeInfo");
        }

        return codeInfo;
    }

    public void initCodes() throws TdbGetDataException {
        shSharesA = getCodes(MARKET_SH_SHARES, CodeType.ID_BT_SHARES_A.getIndex());
        szSharesA = getCodes(MARKET_SZ_SHARES, CodeType.ID_BT_SHARES_A.getIndex());
        szSharesS = getCodes(MARKET_SZ_SHARES, CodeType.ID_BT_SHARES_S.getIndex());
        szSharesG = getCodes(MARKET_SZ_SHARES, CodeType.ID_BT_SHARES_G.getIndex());
        szSharesAll = Util.concatArray(szSharesA, szSharesS, szSharesG);
        shIndex = getCodes(MARKET_SH_SHARES, CodeType.ID_BT_INDEX.getIndex());
        szIndex = getCodes(MARKET_SZ_SHARES, CodeType.ID_BT_INDEX.getIndex());
        shSharesAndIndex = Util.concatArray(shSharesA, shIndex);
        szSharesAndIndex = Util.concatArray(szSharesAll, szIndex);
        allSharesAndIndex = Util.concatArray(shSharesAndIndex, szSharesAndIndex);
    }

    public KLine[] getKLines(String code, String market, int cqflag, int cyctype, int cycdef, int autoComplete,
                             int beginDate, int endDate, int beginTime, int endtime) throws TdbGetDataException {
        ReqKLine reqKL = new ReqKLine();
        reqKL.setCode(code);
        reqKL.setMarketKey(market);
        reqKL.setCQFlag(cqflag);
        reqKL.setCycType(cyctype);
        reqKL.setCycDef(cycdef);
        reqKL.setAutoComplete(autoComplete);
        reqKL.setBeginDate(beginDate);
        reqKL.setEndDate(endDate);
        reqKL.setBeginTime(beginTime);
        reqKL.setEndTime(endtime);

        KLine[] klines = client.getKLine(reqKL);

        if (klines.length == 0) {
            throw new TdbGetDataException("Return null with getKLines");
        }

        return klines;
    }

    public KLine[] getKLines(String code, String market, int cyctype, int cycdef, int beginDate, int endDate,
                             int beginTime, int endtime) throws TdbGetDataException {
        return getKLines(code, market, REFILLFLAG.REFILL_BACKWARD, cyctype, cycdef, 0, beginDate, endDate, beginTime,
                endtime);
    }

    public KLine[] getKLines(String code, String market, int cyctype, int cycdef, int beginDate,
                             int endDate) throws TdbGetDataException {
        return getKLines(code, market, REFILLFLAG.REFILL_BACKWARD, cyctype, cycdef, 0, beginDate, endDate, 0, 0);
    }

    public Code[] getShSharesA() {
        return shSharesA;
    }

    public Code[] getSzSharesA() {
        return szSharesA;
    }

    public Code[] getSzSharesS() {
        return szSharesS;
    }

    public Code[] getSzSharesG() {
        return szSharesG;
    }

    public Code[] getSzSharesAll() {
        return szSharesAll;
    }

    public Code[] getShIndex() {
        return shIndex;
    }

    public Code[] getSzIndex() {
        return szIndex;
    }

    public Code[] getShSharesAndIndex() {
        return shSharesAndIndex;
    }

    public Code[] getSzSharesAndIndex() {
        return szSharesAndIndex;
    }

    public Code[] getAllSharesAndIndex() {
        return allSharesAndIndex;
    }

    public static void printCode(Code code) {
        logger.info("CODE: " + code.getWindCode() + ", " + code.getMarket() + ", " + code.getCode() + ", "
                + code.getENName() + ", " + code.getCNName() + ", " + code.getType());
    }

    public static void printCodes(Code[] codes) {
        for (Code code : codes) {
            printCode(code);
        }
    }

    public static void printKlines(KLine[] klines) {
        logger.info("Success to get " + klines.length + " klines for " + klines[0].getCode());
        for (KLine k : klines) {
            logger.info(k.getWindCode() + ", " + k.getDate() + ", " + k.getTime() + ", " + k.getOpen() + ", "
                    + k.getHigh() + ", " + k.getLow() + ", " + k.getClose() + ", " + k.getVolume() + ", "
                    + k.getTurover() + ", " + k.getMatchItems() + ", " + k.getInterest());
        }
    }
}
