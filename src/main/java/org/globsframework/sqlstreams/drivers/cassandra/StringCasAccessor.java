package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.StringAccessor;

public class StringCasAccessor implements CasAccessor, StringAccessor {
    private int index;
    private CassandraGlobStream stream;

    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public String getString() {
        return stream.getString(index);
    }

    public boolean wasNull() {
        return stream.isNull(index);
    }

    public Object getObjectValue() {
        return getString();
    }
}
