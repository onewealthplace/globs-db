package org.globsframework.sqlstreams.drivers.cassandra.impl;

import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.drivers.cassandra.*;

public class FieldToCassandraAccessorVisitor implements FieldVisitor {
    private CasAccessor accessor;

    public CasAccessor getAccessor() {
        return accessor;
    }

    public void visitInteger(IntegerField field) throws Exception {
        accessor = new IntegerCasAccessor();
    }

    public void visitLong(LongField field) throws Exception {
        accessor = new LongCasAccessor();
    }

    public void visitDouble(DoubleField field) throws Exception {
        accessor = new DoubleCasAccessor();
    }

    public void visitString(StringField field) throws Exception {
        accessor = new StringCasAccessor();
    }

    public void visitBoolean(BooleanField field) throws Exception {
        accessor = new BooleanCasAccessor();
    }

    public void visitBlob(BlobField field) throws Exception {
        accessor = new BlobCasAccessor();
    }

}
