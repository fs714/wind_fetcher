package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import cn.com.wind.td.tdb.KLine;
import com.fs.iquant.wind_fetcher.mongodb.ColBase;
import com.fs.iquant.wind_fetcher.mongodb.DocKLine;
import com.fs.iquant.wind_fetcher.tdb.enums.CycType;
import com.fs.iquant.wind_fetcher.tdb.enums.RefillFlag;
import com.fs.iquant.wind_fetcher.util.Util;
import com.fs.iquant.wind_fetcher.util.WindDateTime;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;

public class KlineRecorder {
    public static final int WIND_AVAILABLE_YEARS = -3;

    private static Logger logger = Logger.getLogger(Tdb.class.getCanonicalName());
    private Tdb tdb;
    private ColBase klineCol;
    private Code code;

    public KlineRecorder(Tdb tdb, MongoDatabase db, Code code) {
        this.tdb = tdb;
        this.code = code;
        String[] windCode = code.getWindCode().split("\\.");
        this.klineCol = new ColBase(db, windCode[1] + "_" + windCode[0]);
    }

    public void saveNoOverwrite(int refillFlag, int cycType, int cycDef, Date beginDate, Date endDate) throws ParseException {
        WindDateTime begin = new WindDateTime(beginDate);
        WindDateTime end = new WindDateTime(endDate);
        KLine[] klines = tdb.getKLines(code.getWindCode(), code.getMarket(), refillFlag, cycType, cycDef,
                begin.getWindDate(), end.getWindDate(), begin.getWindTime(), end.getWindTime());
        if (klines != null) {
            for (KLine kl : klines) {
                Document doc = klineCol.getCol().find(and(eq("refillFlag", refillFlag), eq("cycType", cycType),
                        eq("cycDef", cycDef), eq("date", kl.getDate()))).first();
                if (doc == null) {
                    DocKLine docKLine = new DocKLine(code.getWindCode(), code.getCode(), code.getCNName(), code.getMarket(),
                            refillFlag, cycType, cycDef, kl.getDate(), kl.getTime(), kl.getOpen(), kl.getHigh(), kl.getLow(),
                            kl.getClose(), kl.getVolume(), kl.getTurover(), kl.getMatchItems());
                    klineCol.getCol().insertOne(docKLine.getDocument());
                }
            }
        }
    }

    public void saveNoOverwrite(int cycType, Date beginDate, Date endDate) throws ParseException {
        saveNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, beginDate, endDate);
    }

    public void saveAndOverwrite(int refillFlag, int cycType, int cycDef, Date beginDate, Date endDate) throws ParseException {
        WindDateTime begin = new WindDateTime(beginDate);
        WindDateTime end = new WindDateTime(endDate);
        KLine[] klines = tdb.getKLines(code.getWindCode(), code.getMarket(), refillFlag, cycType, cycDef,
                begin.getWindDate(), end.getWindDate(), begin.getWindTime(), end.getWindTime());
        if (klines != null) {
            for (KLine kl : klines) {
                Document doc = klineCol.getCol().find(and(eq("refillFlag", refillFlag), eq("cycType", cycType),
                        eq("cycDef", cycDef), eq("date", new WindDateTime(kl.getDate(), kl.getTime()).getDate()))).first();
                if (doc == null) {
                    DocKLine docKLine = new DocKLine(code.getWindCode(), code.getCode(), code.getCNName(), code.getMarket(),
                            refillFlag, cycType, cycDef, kl.getDate(), kl.getTime(), kl.getOpen(), kl.getHigh(), kl.getLow(),
                            kl.getClose(), kl.getVolume(), kl.getTurover(), kl.getMatchItems());
                    klineCol.getCol().insertOne(docKLine.getDocument());
                } else {
                    DocKLine docKLine = new DocKLine(doc);
                    docKLine.setOpen(kl.getOpen());
                    docKLine.setHigh(kl.getHigh());
                    docKLine.setLow(kl.getLow());
                    docKLine.setClose(kl.getClose());
                    docKLine.setVolume(kl.getVolume());
                    docKLine.setAmount(kl.getTurover());
                    docKLine.setTransNum(kl.getMatchItems());
                    klineCol.getCol().replaceOne(doc, docKLine.getDocument());
                }
            }
        }
    }

    public void saveAndOverwrite(int cycType, Date beginDate, Date endDate) throws ParseException {
        saveAndOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, beginDate, endDate);
    }

    public void saveNewNoOverwrite(int refillFlag, int cycType, int cycDef, Date beginDate, Date endDate) throws ParseException {
        Document doc = klineCol.getCol().find(and(eq("refillFlag", refillFlag), eq("cycType", cycType),
                eq("cycDef", cycDef))).sort(descending("date")).first();
        if (doc == null) {
            saveNoOverwrite(refillFlag, cycType, cycDef, beginDate, endDate);
        } else {
            Date latestDate = (Date) doc.get("date");
            if (endDate.after(latestDate)) {
                if (beginDate.after(latestDate)) {
                    saveNoOverwrite(refillFlag, cycType, cycDef, beginDate, endDate);
                } else {
                    Calendar c = Calendar.getInstance();
                    c.setTime(latestDate);
                    c.add(Calendar.MILLISECOND, 1);
                    saveNoOverwrite(refillFlag, cycType, cycDef, c.getTime(), endDate);
                }
            }
        }
    }

    public void saveNewNoOverwrite(int cycType, Date beginDate, Date endDate) throws ParseException {
        saveNewNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, beginDate, endDate);
    }

    public void saveHistoryNoOverwrite(int refillFlag, int cycType, int cycDef, Date begindate, Date endDate)
            throws ParseException {
        Date begin = begindate;
        Date end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30);
        while (begin.before(endDate)) {
            saveNoOverwrite(refillFlag, cycType, cycDef, begin, end);
            begin = end;
            end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30);
        }
    }

    public void saveHistoryNoOverwrite(int cycType, Date begindate, Date endDate)
            throws ParseException {
        saveHistoryNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, begindate, endDate);
    }

    public void saveHistoryNoOverwrite(int refillFlag, int cycType, int cycDef) throws ParseException {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.add(Calendar.YEAR, WIND_AVAILABLE_YEARS);

        saveHistoryNoOverwrite(refillFlag, cycType, cycDef, c.getTime(), new Date());
    }

    public void saveHistoryNoOverwrite(int cycType) throws ParseException {
        saveHistoryNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1);
    }

    public void saveHistoryAndOverwrite(int refillFlag, int cycType, int cycDef, Date begindate, Date endDate)
            throws ParseException {
        Date begin = begindate;
        Date end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30);
        while (begin.before(endDate)) {
            saveAndOverwrite(refillFlag, cycType, cycDef, begin, end);
            begin = end;
            end = Util.dateAdd(begin, Calendar.DAY_OF_MONTH, 30);
        }
    }

    public void saveHistoryAndOverwrite(int cycType, Date begindate, Date endDate)
            throws ParseException {
        saveHistoryAndOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, begindate, endDate);
    }

    public void saveHistoryAndOverwrite(int refillFlag, int cycType, int cycDef) throws ParseException {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.add(Calendar.YEAR, WIND_AVAILABLE_YEARS);

        saveHistoryAndOverwrite(refillFlag, cycType, cycDef, c.getTime(), new Date());
    }

    public void saveHistoryAndOverwrite(int cycType) throws ParseException {
        saveHistoryAndOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1);
    }

    public void saveNewHistoryNoOverwrite(int refillFlag, int cycType, int cycDef, Date beginDate, Date endDate)
            throws ParseException {
        Document doc = klineCol.getCol().find(and(eq("refillFlag", refillFlag), eq("cycType", cycType),
                eq("cycDef", cycDef))).sort(descending("date")).first();
        if (doc == null) {
            saveHistoryNoOverwrite(refillFlag, cycType, cycDef, beginDate, endDate);
        } else {
            Date latestDate = (Date) doc.get("date");
            if (endDate.after(latestDate)) {
                if (beginDate.after(latestDate)) {
                    saveHistoryNoOverwrite(refillFlag, cycType, cycDef, beginDate, endDate);
                } else {
                    Calendar c = Calendar.getInstance();
                    c.setTime(latestDate);
                    c.add(Calendar.MILLISECOND, 1);
                    saveHistoryNoOverwrite(refillFlag, cycType, cycDef, c.getTime(), endDate);
                }
            }
        }
    }

    public void saveNewHistoryNoOverwrite(int cycType, Date begindate, Date endDate)
            throws ParseException {
        saveNewHistoryNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1, begindate, endDate);
    }

    public void saveNewHistoryNoOverwrite(int refillFlag, int cycType, int cycDef) throws ParseException {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.add(Calendar.YEAR, WIND_AVAILABLE_YEARS);

        saveNewHistoryNoOverwrite(refillFlag, cycType, cycDef, c.getTime(), new Date());
    }

    public void saveNewHistoryNoOverwrite(int cycType) throws ParseException {
        saveNewHistoryNoOverwrite(RefillFlag.REFILL_BACKWARD.getFlag(), cycType, 1);
    }

    public void saveAllNoOverwrite(int refillFlag, int cycDef, Date begindate, Date endDate) throws ParseException {
        for (CycType cycType : CycType.values()) {
            saveHistoryNoOverwrite(refillFlag, cycType.getFlag(), cycDef, begindate, endDate);
        }
    }

    public void saveAllNoOverwrite(Date begindate, Date endDate) throws ParseException {
        for (CycType cycType : CycType.values()) {
            saveHistoryNoOverwrite(cycType.getFlag(), begindate, endDate);
        }
    }

    public void saveAllNoOverwrite(int refillFlag, int cycDef) throws ParseException {
        for (CycType cycType : CycType.values()) {
            saveHistoryNoOverwrite(refillFlag, cycType.getFlag(), cycDef);
        }
    }

    public void saveAllNoOverwrite() throws ParseException {
        for (CycType cycType : CycType.values()) {
            saveHistoryNoOverwrite(cycType.getFlag());
        }
    }

    public void drop() {
        klineCol.getCol().drop();
    }

    public ColBase getKlineCol() {
        return klineCol;
    }

    public Code getCode() {
        return code;
    }
}
