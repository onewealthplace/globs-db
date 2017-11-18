package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;

public class KeyStringMongoAccessor implements StringAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public KeyStringMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String getString() {
        ObjectId objectId = currentDoc.get().getObjectId(columnName);
        return objectId.toString();
    }

    public Object getObjectValue() {
        return getString();
    }
}
