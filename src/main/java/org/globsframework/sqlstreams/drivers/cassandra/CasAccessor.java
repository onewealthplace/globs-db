package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.streams.accessors.Accessor;

public interface CasAccessor extends Accessor {
    void setIndex(int index);

    void set(CassandraGlobStream stream);
}
