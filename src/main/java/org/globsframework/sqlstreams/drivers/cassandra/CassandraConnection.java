package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class CassandraConnection implements SqlConnection {

    public SelectBuilder getQueryBuilder(GlobType globType) {
        return new CassandraSelectBuilder(globType, null);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        return new CassandraSelectBuilder(globType, constraint);
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return null;
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        return null;
    }

    public SqlRequest getDeleteRequest(GlobType globType) {
        return null;
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        return null;
    }

    public void commit() throws RollbackFailed, DbConstraintViolation {

    }

    public void commitAndClose() throws RollbackFailed, DbConstraintViolation {

    }

    public void rollbackAndClose() {

    }

    public Connection getConnection() {
        return null;
    }

    public void createTable(GlobType... globType) {

    }

    public void emptyTable(GlobType... globType) {

    }

    public void showDb() {

    }

    public void populate(GlobList all) {

    }

    static class SelectData {
        Field field;
        CasAccessor casAccessor;
    }

    interface CasAccessor {
    }

    static class IntegerCasAccessor implements CasAccessor, IntegerAccessor {
        private final int index;
        private Row row;
        private boolean isNull;

        IntegerCasAccessor(int index) {
            this.index = index;
        }

        public Integer getInteger() {
            return row.isNull(index)? null : row.getInt(index);
        }

        public int getValue(int valueIfNull) {
            return row.isNull(index)? valueIfNull : row.getInt(index);
        }

        public boolean wasNull() {
            return row.isNull(index);
        }

        public Object getObjectValue() {
            return getInteger();
        }
    }

    static class DoubleCasAccessor implements CasAccessor, DoubleAccessor {
        private final int index;
        private Row row;
        private boolean isNull;

        DoubleCasAccessor(int index) {
            this.index = index;
        }

        public Double getDouble() {
            return row.isNull(index)? null : row.getDouble(index);
        }

        public double getValue(double valueIfNull) {
            return row.isNull(index)? valueIfNull : row.getDouble(index);
        }

        public boolean wasNull() {
            return row.isNull(index);
        }

        public Object getObjectValue() {
            return getDouble();
        }
    }

    static class StringCasAccessor implements CasAccessor, StringAccessor {
        private final int index;
        private Row row;
        private boolean isNull;

        StringCasAccessor(int index) {
            this.index = index;
        }

        public String getString() {
            return row.isNull(index) ? null : row.getString(index);
        }

        public boolean wasNull() {
            return row.isNull(index);
        }

        public Object getObjectValue() {
            return getString();
        }
    }

    private static class CassandraSelectBuilder implements SelectBuilder {
        private final GlobType globType;
        private final Constraint constraint;
        private Map<Field, SelectData> selectedField = new HashMap<>();

        public CassandraSelectBuilder(GlobType globType, Constraint constraint) {
            this.globType = globType;
            this.constraint = constraint;
        }

        public SelectQuery getQuery() {
            return null;
        }

        public SelectQuery getNotAutoCloseQuery() {
            return null;
        }

        public SelectBuilder select(Field field) {
            return null;
        }

        public SelectBuilder selectAll() {
            return null;
        }

        public SelectBuilder select(IntegerField field, Ref<IntegerAccessor> accessor) {
            return null;
        }

        public SelectBuilder select(LongField field, Ref<LongAccessor> accessor) {
            return null;
        }

        public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> accessor) {
            return null;
        }

        public SelectBuilder select(StringField field, Ref<StringAccessor> accessor) {
            return null;
        }

        public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> accessor) {
            return null;
        }

        public SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor) {
            return null;
        }

        public SelectBuilder orderAsc(Field field) {
            return null;
        }

        public SelectBuilder orderDesc(Field field) {
            return null;
        }

        public SelectBuilder top(int n) {
            return null;
        }

        public SelectBuilder withKeys() {
            return null;
        }

        public IntegerAccessor retrieve(IntegerField field) {
            return null;
        }

        public LongAccessor retrieve(LongField field) {
            return null;
        }

        public StringAccessor retrieve(StringField field) {
            return null;
        }

        public BooleanAccessor retrieve(BooleanField field) {
            return null;
        }

        public DoubleAccessor retrieve(DoubleField field) {
            return null;
        }

        public BlobAccessor retrieve(BlobField field) {
            return null;
        }

        public Accessor retrieveUnTyped(Field field) {
            return null;
        }
    }
}
