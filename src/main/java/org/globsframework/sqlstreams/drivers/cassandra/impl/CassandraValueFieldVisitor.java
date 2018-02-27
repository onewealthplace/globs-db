package org.globsframework.sqlstreams.drivers.cassandra.impl;

import com.datastax.driver.core.BoundStatement;
import org.globsframework.metamodel.fields.*;

import java.nio.ByteBuffer;


public class CassandraValueFieldVisitor implements FieldVisitor {
    private final BoundStatement boundStatement;
    private Object value;
    private int index;

    public CassandraValueFieldVisitor(BoundStatement boundStatement) {
        this.boundStatement = boundStatement;
    }

    public void setValue(Object value, int index) {
        this.value = value;
        this.index = index;
    }

    public void visitInteger(IntegerField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setInt(index, (Integer) value);
        }
    }

    public void visitLong(LongField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setLong(index, (Long) value);
        }
    }

    public void visitDouble(DoubleField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setDouble(index, (Double) value);
        }
    }

    public void visitString(StringField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setString(index, (String) value);
        }
    }

    public void visitBoolean(BooleanField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setBool(index, (Boolean) value);
        }
    }

    public void visitBlob(BlobField field) throws Exception {
        if (value == null) {
            boundStatement.setToNull(index);
        } else {
            boundStatement.setBytesUnsafe(index, ByteBuffer.wrap(((byte[]) value)));
        }
    }
}
