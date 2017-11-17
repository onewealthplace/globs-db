package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.LongAccessor;
import org.globsframework.utils.Ref;

public class LongMongoAccessor implements LongAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public LongMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Long getLong() {
        return currentDoc.get().getLong(columnName);
    }

    public long getValue(long valueIfNull) {
        Long value = getLong();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getObjectValue() == null;
    }

    public Object getObjectValue() {
        return getLong();
    }
}
