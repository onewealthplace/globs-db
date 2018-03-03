package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.globsframework.sqlstreams.drivers.mongodb.MongoUtils;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefStringMongoAccessor implements StringAccessor {
    private static Logger LOGGER = LoggerFactory.getLogger(RefStringMongoAccessor.class);
    private final String columnName;
    private final Ref<Document> currentDoc;

    public RefStringMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String getString() {
        Document document = currentDoc.get().get(columnName, Document.class);
        if (document != null) {
            ObjectId objectId = document.getObjectId(MongoUtils.DB_REF_ID_EXT);
            if (objectId == null) {
                String message = "Null value for " + MongoUtils.DB_REF_ID_EXT + " in " + document + " of " + currentDoc.get();
                LOGGER.error(message);
                throw new RuntimeException(message);
            }
            return objectId.toString();
        }
        return null;
    }

    public Object getObjectValue() {
        return getString();
    }
}
