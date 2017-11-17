package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.IntegerAccessor;
import org.globsframework.utils.Ref;

public class IntegerMongoAccessor implements IntegerAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public IntegerMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Integer getInteger() {
        return currentDoc.get().getInteger(columnName);
    }

    public int getValue(int valueIfNull) {
        Integer value = getInteger();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getInteger() == null;
    }

    public Object getObjectValue() {
        return getInteger();
    }
}
