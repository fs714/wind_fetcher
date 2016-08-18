package com.fs.iquant.wind_fetcher.mongodb;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DocKLine extends DocBase {
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

    public DocKLine(String windCode, String code, String name, String market, int refillFlag, int cycType, int cycDef,
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
        toDocument();
    }

    public DocKLine(String windCode, String code, String name, String market, int refillFlag, int cycType, int cycDef,
                    int date, int time, int open, int high, int low, int close, long volume, long amount,
                    int transNum) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date dateTime;
        if (time > 99999999) {
            dateTime = format.parse(Integer.toString(date) + Integer.toString(time));
        } else {
            dateTime = format.parse(Integer.toString(date) + "0" + Integer.toString(time));
        }
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
        toDocument();
    }

    public DocKLine(Document document) {
        this.document = document;
        this._id = (ObjectId) document.get("_id");
        this.windCode = (String) document.get("windCode");
        this.code = (String) document.get("code");
        this.name = (String) document.get("name");
        this.market = (String) document.get("market");
        this.refillFlag = (int) document.get("refillFlag");
        this.cycType = (int) document.get("cycType");
        this.cycDef = (int) document.get("cycDef");
        this.date = (Date) document.get("date");
        this.open = (int) document.get("open");
        this.high = (int) document.get("high");
        this.low = (int) document.get("low");
        this.close = (int) document.get("close");
        this.volume = (long) document.get("volume");
        this.amount = (long) document.get("amount");
        this.transNum = (int) document.get("transNum");
    }

    private Document toDocument() {
        document = new Document("_id", get_id())
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

    public String toString() {
        return getDate() + "\t" + getDocument().toJson();
    }

    public String getWindCode() {
        return windCode;
    }

    public void setWindCode(String windCode) {
        this.windCode = windCode;
        document.put("windCode", windCode);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
        document.put("code", code);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        document.put("name", name);
    }

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
        document.put("market", market);
    }

    public int getRefillFlag() {
        return refillFlag;
    }

    public void setRefillFlag(int refillFlag) {
        this.refillFlag = refillFlag;
        document.put("refillFlag", refillFlag);
    }

    public int getCycType() {
        return cycType;
    }

    public void setCycType(int cycType) {
        this.cycType = cycType;
        document.put("cycType", cycType);
    }

    public int getCycDef() {
        return cycDef;
    }

    public void setCycDef(int cycDef) {
        this.cycDef = cycDef;
        document.put("cycDef", cycDef);
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
        document.put("date", date);
    }

    public int getOpen() {
        return open;
    }

    public void setOpen(int open) {
        this.open = open;
        document.put("open", open);
    }

    public int getHigh() {
        return high;
    }

    public void setHigh(int high) {
        this.high = high;
        document.put("high", high);
    }

    public int getLow() {
        return low;
    }

    public void setLow(int low) {
        this.low = low;
        document.put("low", low);
    }

    public int getClose() {
        return close;
    }

    public void setClose(int close) {
        this.close = close;
        document.put("close", close);
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
        document.put("volume", volume);
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
        document.put("amount", amount);
    }

    public int getTransNum() {
        return transNum;
    }

    public void setTransNum(int transNum) {
        this.transNum = transNum;
        document.put("transNum", transNum);
    }
}
