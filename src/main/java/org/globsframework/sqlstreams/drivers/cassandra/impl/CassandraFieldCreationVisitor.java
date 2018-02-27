package org.globsframework.sqlstreams.drivers.cassandra.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.annotations.AutoIncrementAnnotationType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

public class CassandraFieldCreationVisitor implements FieldVisitor {
    private SqlService sqlService;
    protected StringPrettyWriter prettyWriter;
    private boolean appendComma;

    public CassandraFieldCreationVisitor(SqlService sqlService, StringPrettyWriter prettyWriter) {
        this.sqlService = sqlService;
        this.prettyWriter = prettyWriter;
    }

    public FieldVisitor appendComma(boolean appendComma) {
        this.appendComma = appendComma;
        return this;
    }

    public void visitInteger(IntegerField field) throws Exception {
        add("int", field);
    }

    public void visitLong(LongField field) throws Exception {
        add("bigint", field);
    }

    public void visitDouble(DoubleField field) throws Exception {
        add("double", field);
    }

    public void visitString(StringField field) throws Exception {
        add("varchar", field);
    }

    public void visitBoolean(BooleanField field) throws Exception {
        add("boolean", field);
    }

    public void visitBlob(BlobField field) throws Exception {
        add("blob", field);
    }

    protected void add(String param, Field field) {
        String columnName = sqlService.getColumnName(field);
        if (columnName != null) {
            prettyWriter
                  .append(columnName)
                  .append(" ")
                  .append(param)
                  .appendIf(", ", appendComma);
        }
    }
}
