package com.fs.iquant.wind_fetcher.mongodb;

import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class KLine extends DocBase {
    private String windCode;
    private String code;
    private String name;
    private String market;
    private int refillFlag;
    private int cycType;
    private int cycDef;
    private Date date;
    private int open;
    private int high;
    private int low;
    private int close;
    private long volume;
    private long amount;
    private int transNum;

    public KLine(String windCode, String code, String name, String market, int refillFlag, int cycType, int cycDef,
                 Date date, int open, int high, int low, int close, long volume, long amount, int transNum) {
        this.windCode = windCode;
        this.code = code;
        this.name = name;
        this.market = market;
        this.refillFlag = refillFlag;
        this.cycType = cycType;
        this.cycDef = cycDef;
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.amount = amount;
        this.transNum = transNum;
    }

    public KLine(String windCode, String code, String name, String market, int refillFlag, int cycType, int cycDef,
                 int date, int time, int open, int high, int low, int close, long volume, long amount,
                 int transNum) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.ENGLISH);
        Date dateTime = format.parse(Integer.toString(date) + Integer.toString(time));
        this.windCode = windCode;
        this.code = code;
        this.name = name;
        this.market = market;
        this.refillFlag = refillFlag;
        this.cycType = cycType;
        this.cycDef = cycDef;
        this.date = dateTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.amount = amount;
        this.transNum = transNum;
    }

    private Document toDocument() {
        Document document = new Document("_id", get_id())
                .append("windCode", getWindCode())
                .append("code", getCode())
                .append("name", getName())
                .append("market", getMarket())
                .append("refillFlag", getRefillFlag())
                .append("cycType", getCycType())
                .append("cycDef", getCycDef())
                .append("date", getDate())
                .append("open", getOpen())
                .append("high", getHigh())
                .append("low", getLow())
                .append("close", getClose())
                .append("volume", getVolume())
                .append("amount", getAmount())
                .append("transNum", getTransNum());
        return document;
    }

    public String getWindCode() {
        return windCode;
    }

    public void setWindCode(String windCode) {
        this.windCode = windCode;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
    }

    public int getRefillFlag() {
        return refillFlag;
    }

    public void setRefillFlag(int refillFlag) {
        this.refillFlag = refillFlag;
    }

    public int getCycType() {
        return cycType;
    }

    public void setCycType(int cycType) {
        this.cycType = cycType;
    }

    public int getCycDef() {
        return cycDef;
    }

    public void setCycDef(int cycDef) {
        this.cycDef = cycDef;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getOpen() {
        return open;
    }

    public void setOpen(int open) {
        this.open = open;
    }

    public int getHigh() {
        return high;
    }

    public void setHigh(int high) {
        this.high = high;
    }

    public int getLow() {
        return low;
    }

    public void setLow(int low) {
        this.low = low;
    }

    public int getClose() {
        return close;
    }

    public void setClose(int close) {
        this.close = close;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public int getTransNum() {
        return transNum;
    }

    public void setTransNum(int transNum) {
        this.transNum = transNum;
    }
}
