package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.BooleanAccessor;
import org.globsframework.utils.Ref;

public class BooleanMongoAccessor implements BooleanAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public BooleanMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Boolean getBoolean() {
        return currentDoc.get().getBoolean(columnName);
    }

    public boolean getValue(boolean b) {
        return currentDoc.get().getBoolean(columnName, b);
    }

    public Object getObjectValue() {
        return getBoolean();
    }
}
