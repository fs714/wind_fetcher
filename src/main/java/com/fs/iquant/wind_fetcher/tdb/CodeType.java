package com.fs.iquant.wind_fetcher.tdb;

public enum CodeType {
    ID_BT_INDEX(0x01),
    ID_BT_INDEX_ASIA(0x03),
    ID_BT_INDEX_FOREIGN(0x04),
    ID_BT_INDEX_HH(0x05),
    ID_BT_INDEX_USER(0x06),

    ID_BT_SHARES_A(0x10),
    ID_BT_SHARES_S(0x11),
    ID_BT_SHARES_G(0x12),

    ID_BT_OTHER_SHARES_SG_A(0xf1),
    ID_BT_OTHER_SHARES_ZF_A(0xf2);

    private int index;

    private CodeType(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
