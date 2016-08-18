package com.fs.iquant.wind_fetcher.mongodb;

import org.bson.Document;
import org.bson.types.ObjectId;

public class DocCode extends DocBase {
    private String windCode;
    private String code;
    private String market;
    private String name;
    private int type;

    public DocCode(String windCode, String code, String market, String name, int type) {
        this.windCode = windCode;
        this.code = code;
        this.market = market;
        this.name = name;
        this.type = type;
        toDocument();
    }

    public DocCode(Document document) {
        this.document = document;
        this._id = (ObjectId) document.get("_id");
        this.windCode = (String) document.get("windCode");
        this.code = (String) document.get("code");
        this.market = (String) document.get("market");
        this.name = (String) document.get("name");
        this.type = (int) document.get("type");
    }

    private Document toDocument() {
        document = new Document("_id", get_id())
                .append("windCode", getWindCode())
                .append("code", getCode())
                .append("market", getMarket())
                .append("name", getName())
                .append("type", getType());
        return document;
    }

    public String toString() {
        return getDocument().toJson();
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

    public String getMarket() {
        return market;
    }

    public void setMarket(String market) {
        this.market = market;
        document.put("market", market);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        document.put("name", name);
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
        document.put("type", type);
    }
}
