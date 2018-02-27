package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.Session;
import org.globsframework.directory.Directory;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.accessors.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.cassandra.impl.FieldToCassandraAccessorVisitor;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.sql.Connection;
import java.util.*;

public class CassandraConnection implements SqlConnection {
    private Session session;
    private SqlService sqlService;

    public CassandraConnection(Session session, SqlService sqlService) {
        this.session = session;
        this.sqlService = sqlService;
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        return new CassandraSelectBuilder(session, sqlService, globType, null);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        return new CassandraSelectBuilder(session, sqlService, globType, constraint);
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

    public void createTable(GlobType... globType) {

    }

    public void emptyTable(GlobType... globType) {

    }

    public void showDb() {

    }

    public void populate(GlobList all) {

    }

    private static class CassandraSelectBuilder implements SelectBuilder {
        private Session session;
        private SqlService sqlService;
        private final GlobType globType;
        private final Constraint constraint;
        private Map<Field, CasAccessor> fieldToAccessorHolder = new HashMap<>();
        private int top;
        private List<CassandraSelectQuery.Order> orders = new ArrayList<>();
        private Set<Field> distinct = new HashSet<>();

        public CassandraSelectBuilder(Session session, SqlService sqlService, GlobType globType, Constraint constraint) {
            this.session = session;
            this.sqlService = sqlService;
            this.globType = globType;
            this.constraint = constraint;
        }

        public SelectQuery getQuery() {
            return new CassandraSelectQuery(session, constraint, fieldToAccessorHolder,sqlService, true, orders, top, distinct);
        }

        public SelectQuery getNotAutoCloseQuery() {
            return new CassandraSelectQuery(session, constraint, fieldToAccessorHolder,sqlService, true, orders, top, distinct);
        }

        public SelectBuilder select(Field field) {
            FieldToCassandraAccessorVisitor visitor = new FieldToCassandraAccessorVisitor();
            field.safeVisit(visitor);
            fieldToAccessorHolder.put(field, visitor.getAccessor());
            return this;
        }

        public SelectBuilder selectAll() {
            for (Field field : globType.getFields()) {
                select(field);
            }
            return this;
        }

        public SelectBuilder select(IntegerField field, Ref<IntegerAccessor> ref) {
            return createAccessor(field, ref, new IntegerCasAccessor());
        }

        public SelectBuilder select(LongField field, Ref<LongAccessor> accessor) {
            return createAccessor(field, accessor, new LongCasAccessor());
        }

        public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> ref) {
            return createAccessor(field, ref, new BooleanCasAccessor());
        }

        public SelectBuilder select(StringField field, Ref<StringAccessor> ref) {
            return createAccessor(field, ref, new StringSqlAccessor());
        }

        public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> ref) {
            return createAccessor(field, ref, new DoubleSqlAccessor());
        }

        public SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor) {
            return createAccessor(field, accessor, new BlobSqlAccessor());
        }

        public SelectBuilder orderAsc(Field field) {
            orders.add(new CassandraSelectQuery.Order(field, true));
            return this;
        }

        public SelectBuilder orderDesc(Field field) {
            orders.add(new CassandraSelectQuery.Order(field, false));
            return this;
        }

        public SelectBuilder top(int n) {
            top = n;
            return this;
        }

        public SelectBuilder withKeys() {
            Arrays.stream(globType.getKeyFields()).forEach(this::retrieveUnTyped);
            return this;
        }

        public SelectBuilder distinct(Collection<Field> fields) {
            this.distinct.addAll(fields);
            return this;
        }

        public BooleanAccessor retrieve(BooleanField field) {
            BooleanCasAccessor accessor = new BooleanCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public IntegerAccessor retrieve(IntegerField field) {
            IntegerCasAccessor accessor = new IntegerCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public LongAccessor retrieve(LongField field) {
            LongCasAccessor accessor = new LongCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public StringAccessor retrieve(StringField field) {
            StringCasAccessor accessor = new StringCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public DoubleAccessor retrieve(DoubleField field) {
            DoubleCasAccessor accessor = new DoubleCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public BlobAccessor retrieve(BlobField field) {
            BlobCasAccessor accessor = new BlobCasAccessor();
            fieldToAccessorHolder.put(field, accessor);
            return accessor;
        }

        public Accessor retrieveUnTyped(Field field) {
            AccessorToFieldVisitor visitor = new AccessorToFieldVisitor();
            field.safeVisit(visitor);
            return visitor.get();
        }

        private <T extends Accessor, D extends T> SelectBuilder createAccessor(Field field, Ref<T> ref, D accessor) {
            ref.set(accessor);
            fieldToAccessorHolder.put(field, (CasAccessor) accessor);
            return this;
        }

        private class AccessorToFieldVisitor implements FieldVisitor {
            private Accessor accessor;

            public AccessorToFieldVisitor() {
            }

            public void visitInteger(IntegerField field) throws Exception {
                accessor = retrieve(field);
            }

            public void visitDouble(DoubleField field) throws Exception {
                accessor = retrieve(field);
            }

            public void visitString(StringField field) throws Exception {
                accessor = retrieve(field);
            }

            public void visitBoolean(BooleanField field) throws Exception {
                accessor = retrieve(field);
            }

            public void visitBlob(BlobField field) throws Exception {
                accessor = retrieve(field);
            }

            public void visitLong(LongField field) throws Exception {
                accessor = retrieve(field);
            }

            public Accessor get() {
                return accessor;
            }
        }
    }
}
