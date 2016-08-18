package com.fs.iquant.wind_fetcher.mongodb;

import org.bson.Document;
import org.bson.types.ObjectId;

public abstract class DocBase {
    protected ObjectId _id = new ObjectId();
    protected Document document;

    public DocBase() {
    }

    public DocBase(Document document) {
        this.document = document;
    }

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(ObjectId _id) {
        this._id = _id;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }
}
