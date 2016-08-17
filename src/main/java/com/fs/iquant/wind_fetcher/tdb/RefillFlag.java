package com.fs.iquant.wind_fetcher.tdb;

public enum RefillFlag {
    REFILL_NONE(0),
    REFILL_BACKWARD(1),
    REFILL_FORWARD(2);

    private int flag;

    private RefillFlag(int flag) {
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }
}
