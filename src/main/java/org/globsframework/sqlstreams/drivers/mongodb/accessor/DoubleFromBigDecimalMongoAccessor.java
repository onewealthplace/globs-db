package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.globsframework.streams.accessors.DoubleAccessor;
import org.globsframework.utils.Ref;

public class DoubleFromBigDecimalMongoAccessor implements DoubleAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public DoubleFromBigDecimalMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Double getDouble() {
        Object o = currentDoc.get().get(columnName);
        if (o == null) {
            return null;
        }
        if (o instanceof Decimal128) {
            return ((Decimal128) o).bigDecimalValue().doubleValue();
        } else if (o instanceof Double) {
            return (Double) o;
        } else if (o instanceof Number) {
            return ((Number) o).doubleValue();
        } else {
            throw new RuntimeException("Double type expected but got : " + o.getClass() + " column " + columnName + " in document " + currentDoc.toString());
        }
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
