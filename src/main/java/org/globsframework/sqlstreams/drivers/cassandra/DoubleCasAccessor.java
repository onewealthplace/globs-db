package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.DoubleAccessor;

public class DoubleCasAccessor implements CasAccessor, DoubleAccessor {
    private CassandraGlobStream stream;
    private int index;

    public void setIndex(int index) {
        this.index = index;
    }

    public void set(CassandraGlobStream stream) {
        this.stream = stream;
    }

    public Double getDouble() {
        return stream.getDouble(index);
    }

    public double getValue(double valueIfNull) {
        return stream.getDouble(index, valueIfNull);
    }

    public boolean wasNull() {
        return stream.isNull(index);
    }

    public Object getObjectValue() {
        return getDouble();
    }
}
