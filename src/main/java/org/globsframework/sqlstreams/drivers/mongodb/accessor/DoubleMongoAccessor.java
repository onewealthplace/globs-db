package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.DoubleAccessor;
import org.globsframework.utils.Ref;

public class DoubleMongoAccessor implements DoubleAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public DoubleMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Double getDouble() {
        return currentDoc.get().getDouble(columnName);
    }

    public double getValue(double valueIfNull) {
        Double value = getDouble();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getObjectValue() == null;
    }

    public Object getObjectValue() {
        return getDouble();
    }
}
