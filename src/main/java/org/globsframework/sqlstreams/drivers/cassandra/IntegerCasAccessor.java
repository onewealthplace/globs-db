package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.IntegerAccessor;

public class IntegerCasAccessor implements CasAccessor, IntegerAccessor {
    private CassandraGlobStream stream;
    private int index;


    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public Integer getInteger() {
        return stream.getInteger(index);
    }

    public int getValue(int valueIfNull) {
        return stream.getInteger(index, valueIfNull);
    }

    public boolean wasNull() {
        return stream.isNull(index);
    }

    public Object getObjectValue() {
        return getInteger();
    }
}
