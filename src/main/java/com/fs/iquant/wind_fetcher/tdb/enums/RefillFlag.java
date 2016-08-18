package com.fs.iquant.wind_fetcher.tdb.enums;

public enum RefillFlag {
    REFILL_NONE(0),
    REFILL_BACKWARD(1),
    REFILL_FORWARD(2);

    private int flag;

    RefillFlag(int flag) {
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }
}
