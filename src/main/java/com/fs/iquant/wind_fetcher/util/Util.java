package com.fs.iquant.wind_fetcher.util;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class Util {
    public Util() {
    }

    public static <T> T[] concatArray(T[] first, T[]... rest) {
        int totalLength = first.length;
        for (T[] array : rest) {
            totalLength += array.length;
        }
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static Date dateAdd(Date date, int type, int num) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(type, num);
        return c.getTime();
    }
}
