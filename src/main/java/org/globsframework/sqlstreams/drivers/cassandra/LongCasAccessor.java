package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.LongAccessor;

public class LongCasAccessor implements CasAccessor, LongAccessor {
    private CassandraGlobStream stream;
    private int index;


    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public Long getLong() {
        return stream.getLong(index);
    }

    public long getValue(long valueIfNull) {
        return stream.getLong(index, valueIfNull);
    }

    public boolean wasNull() {
        return stream.isNull(index);
    }

    public Object getObjectValue() {
        return getLong();
    }
}
