package com.fs.iquant.wind_fetcher.tdb.enums;

public enum CycType {
    CYC_MINUTE(1),
    CYC_DAY(2),
    CYC_WEEK(3),
    CYC_MONTH(4),
    CYC_SEASON(5),
    CYC_HAFLYEAR(6),
    CYC_YEAR(7);

    private int flag;

    CycType(int flag) {
        this.flag = flag;
    }

    public int getFlag() {
        return flag;
    }
}
