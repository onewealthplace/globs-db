package org.globsframework.sqlstreams.drivers.cassandra.impl;

import com.datastax.driver.core.BoundStatement;
import org.globsframework.metamodel.fields.*;

import java.nio.ByteBuffer;


public class SqlValueFieldVisitor implements FieldVisitor {
    private final BoundStatement bind;
    private Object value;
    private int index;

    public SqlValueFieldVisitor(BoundStatement bind) {
        this.bind = bind;
    }

    public void setValue(Object value, int index) {
        this.value = value;
        this.index = index;
    }

    public void visitInteger(IntegerField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setInt(index, (Integer) value);
        }
    }

    public void visitLong(LongField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setLong(index, (Long) value);
        }
    }

    public void visitDouble(DoubleField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setDouble(index, (Double) value);
        }
    }

    public void visitString(StringField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setString(index, (String) value);
        }
    }

    public void visitBoolean(BooleanField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setBool(index, (Boolean) value);
        }
    }

    public void visitBlob(BlobField field) throws Exception {
        if (value == null) {
            bind.setToNull(index);
        } else {
            bind.setBytesUnsafe(index, ByteBuffer.wrap(((byte[]) value)));
        }
    }
}
