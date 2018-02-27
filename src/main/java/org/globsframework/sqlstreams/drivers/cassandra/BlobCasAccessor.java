package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.BlobAccessor;

public class BlobCasAccessor implements CasAccessor, BlobAccessor {
    private CassandraGlobStream stream;
    private int index;

    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public byte[] getValue() {
        return stream.getBytes(index);
    }

    public Object getObjectValue() {
        return getValue();
    }
}
