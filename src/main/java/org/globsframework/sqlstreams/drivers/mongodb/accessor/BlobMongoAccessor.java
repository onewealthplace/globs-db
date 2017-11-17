package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.BlobAccessor;
import org.globsframework.utils.Ref;

import java.util.Base64;

public class BlobMongoAccessor implements BlobAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public BlobMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public byte[] getValue() {
        return Base64.getDecoder().decode(currentDoc.get().getString(columnName));
    }

    public Object getObjectValue() {
        return getValue();
    }
}
