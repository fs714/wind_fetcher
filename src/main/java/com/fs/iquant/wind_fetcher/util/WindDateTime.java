package com.fs.iquant.wind_fetcher.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WindDateTime {
    private Date date;
    private int windDate;
    private int windTime;

    public WindDateTime(Date date) {
        this.date = date;
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd:HHmmssSSS");
        String[] windDateTime = dateFormat.format(date).split(":");
        this.windDate = Integer.parseInt(windDateTime[0]);
        this.windTime = Integer.parseInt(windDateTime[1]);
    }

    public WindDateTime(int windDate, int windTime) throws ParseException {
        this.windDate = windDate;
        this.windTime = windTime;
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        date = format.parse(String.format("%08d", windDate) + String.format("%09d", windTime));
    }

    public Date getDate() {
        return date;
    }

    public int getWindDate() {
        return windDate;
    }

    public int getWindTime() {
        return windTime;
    }
}
