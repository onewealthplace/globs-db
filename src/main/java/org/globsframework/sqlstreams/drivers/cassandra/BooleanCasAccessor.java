package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.BooleanAccessor;

public class BooleanCasAccessor implements CasAccessor, BooleanAccessor {
    private CassandraGlobStream stream;
    private int index;


    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public boolean getValue(boolean valueIfNull) {
        return stream.getBoolean(index, valueIfNull);
    }

    public Boolean getBoolean() {
        return stream.getBoolean(index);
    }

    public Object getObjectValue() {
        return getBoolean();
    }
}
