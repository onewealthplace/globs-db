package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.globsframework.metamodel.Field;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.Accessor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class CassandraGlobStream implements GlobStream {
    private final Iterator<Row> iterator;
    private Row row;
    private int rowId = 0;
    private Map<Field, CasAccessor> fieldToAccessorHolder;
    private CassandraSelectQuery query;

    public CassandraGlobStream(ResultSet resultSet, Map<Field, CasAccessor> fieldToAccessorHolder, CassandraSelectQuery query) {
        iterator = resultSet.iterator();
        this.fieldToAccessorHolder = fieldToAccessorHolder;
        this.query = query;
        for (CasAccessor sqlAccessor : fieldToAccessorHolder.values()) {
            sqlAccessor.set(this);
        }
    }

    public boolean next() {
        rowId++;
        boolean hasNext = iterator.hasNext();
        if (!hasNext) {
            close();
        } else {
            row = iterator.next();
        }
        return hasNext;
    }

    public void close() {
        query.resultSetClose();
    }


    public Collection<Field> getFields() {
        return fieldToAccessorHolder.keySet();
    }

    public Accessor getAccessor(Field field) {
        return fieldToAccessorHolder.get(field);
    }

    public double getDouble(int index, double valueIfNull) {
        double aDouble = row.getDouble(index);
        if (aDouble == 0.0 && row.isNull(index)) {
            return valueIfNull;
        } else {
            return aDouble;
        }
    }

    public Double getDouble(int index) {
        double aDouble = row.getDouble(index);
        if (aDouble == 0.0 && row.isNull(index)) {
            return null;
        } else {
            return aDouble;
        }
    }

    public boolean getBoolean(int index, boolean valueIfNull) {
        boolean value = row.getBool(index);
        if (!value && row.isNull(index)) {
            return valueIfNull;
        } else {
            return value;
        }
    }

    public Boolean getBoolean(int index) {
        boolean value = row.getBool(index);
        if (!value && row.isNull(index)) {
            return null;
        } else {
            return value;
        }
    }

    public int getInteger(int index, int valueIfNull) {
        int value = row.getInt(index);
        if (value == 0 && row.isNull(index)) {
            return valueIfNull;
        } else {
            return value;
        }
    }

    public Integer getInteger(int index) {
        int value = row.getInt(index);
        if (value == 0 && row.isNull(index)) {
            return null;
        } else {
            return value;
        }
    }

    public String getString(int index) {
        String value = row.getString(index);
        if (value == null) {
            return null;
        } else {
            return value;
        }

    }

    public byte[] getBytes(int index) {
        ByteBuffer bytes = row.getBytes(index);
        if (bytes != null){
            return bytes.array();
        }
        else {
            return null;
        }
    }

    public long getLong(int index, long valueIfNull) {
        long value = row.getLong(index);
        if (value == 0L && row.isNull(index)) {
            return valueIfNull;
        } else {
            return value;
        }
    }

    public Long getLong(int index) {
        long value = row.getLong(index);
        if (value == 0L && row.isNull(index)) {
            return null;
        } else {
            return value;
        }
    }

    public boolean isNull(int index) {
        return row.isNull(index);
    }
}
