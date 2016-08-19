package com.fs.iquant.wind_fetcher.tdb;

import cn.com.wind.td.tdb.Code;
import com.fs.iquant.wind_fetcher.mongodb.ColBase;
import com.fs.iquant.wind_fetcher.mongodb.DocCode;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

public class CodeRecorder {
    private static final String CODE_DB_NAME = "wind_code";
    private static Logger logger = Logger.getLogger(Tdb.class.getCanonicalName());
    private ColBase codeCol;
    private Tdb tdb;

    public CodeRecorder(Tdb tdb, MongoDatabase db) {
        this.tdb = tdb;
        this.codeCol = new ColBase(db, CODE_DB_NAME);
    }

    public void save() {
        Code[] codes = tdb.getAllSharesAndIndex();
        for (Code code : codes) {
            DocCode docCode = new DocCode(code.getWindCode(), code.getCode(), code.getMarket(), code.getCNName(),
                    code.getType());
            codeCol.getCol().insertOne(docCode.getDocument());
        }
    }

    public void drop() {
        codeCol.getCol().drop();
    }

    public ColBase getCodeCol() {
        return codeCol;
    }
}
