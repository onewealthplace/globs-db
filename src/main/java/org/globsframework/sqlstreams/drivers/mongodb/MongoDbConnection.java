package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.streams.accessors.*;
import org.globsframework.streams.accessors.utils.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDbConnection implements SqlConnection {
    MongoDatabase mongoDatabase;
    MongoDbService sqlService;

    public MongoDbConnection(MongoDatabase mongoDatabase, MongoDbService sqlService) {
        this.mongoDatabase = mongoDatabase;
        this.sqlService = sqlService;
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        return new MongoSelectBuilder(mongoDatabase, globType, sqlService, null);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        return new MongoSelectBuilder(mongoDatabase, globType, sqlService, constraint);
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return new MongoCreateBuilder(mongoDatabase, globType, sqlService);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        return new MongoUpdateBuilder(mongoDatabase, globType, sqlService, constraint);
    }

    public SqlRequest getDeleteRequest(GlobType globType) {
        throw new RuntimeException("Not Implemented");
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        throw new RuntimeException("Not Implemented");
    }

    public void commit() throws RollbackFailed, DbConstraintViolation {
    }

    public void commitAndClose() throws RollbackFailed, DbConstraintViolation {

    }

    public void rollbackAndClose() {

    }

    public void createTable(GlobType... globType) {
        throw new RuntimeException("Not Implemented");
    }

    public void emptyTable(GlobType... globType) {
        throw new RuntimeException("Not Implemented");
    }

    public void showDb() {
    }

    public void populate(GlobList all) {
        MongoUtils.fill(all, sqlService);
    }

    public interface IsComplete {
        boolean complete();
    }

    public static boolean waitComplete(Object thisObject, IsComplete isComplete, int timeInSecond) {
        synchronized (thisObject) {
            long waitUntil = System.currentTimeMillis() + timeInSecond * 1000;
            while (!isComplete.complete()) {
                long stillToWait = waitUntil - System.currentTimeMillis();
                if (stillToWait <= 0) {
                    return false;
                }
                try {
                    thisObject.wait(stillToWait);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }
        return true;
    }

    private static class MongoUpdateBuilder implements UpdateBuilder {
        private final MongoDatabase mongoDatabase;
        private final GlobType globType;
        private SqlService sqlService;
        private final Constraint constraint;
        private final Map<Field, Accessor> fieldsValues = new HashMap<>();

        public MongoUpdateBuilder(MongoDatabase mongoDatabase, GlobType globType, SqlService sqlService, Constraint constraint) {
            this.mongoDatabase = mongoDatabase;
            this.globType = globType;
            this.sqlService = sqlService;
            this.constraint = constraint;
        }

        public UpdateBuilder updateUntyped(Field field, Object value) {
            field.safeVisit(new FieldValueVisitor() {
                public void visitInteger(IntegerField field, Integer value) throws Exception {
                    update(field, value);
                }

                public void visitDouble(DoubleField field, Double value) throws Exception {
                    update(field, value);
                }

                public void visitString(StringField field, String value) throws Exception {
                    update(field, value);
                }

                public void visitBoolean(BooleanField field, Boolean value) throws Exception {
                    update(field, value);
                }

                public void visitLong(LongField field, Long value) throws Exception {
                    update(field, value);
                }

                public void visitBlob(BlobField field, byte[] value) throws Exception {
                    update(field, value);
                }
            }, value);

            return this;
        }

        public UpdateBuilder updateUntyped(Field field, Accessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(IntegerField field, IntegerAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(IntegerField field, Integer value) {
            fieldsValues.put(field, new ValueIntegerAccessor(value));
            return this;
        }

        public UpdateBuilder update(LongField field, LongAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(LongField field, Long value) {
            fieldsValues.put(field, new ValueLongAccessor(value));
            return this;
        }

        public UpdateBuilder update(DoubleField field, DoubleAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(DoubleField field, Double value) {
            fieldsValues.put(field, new ValueDoubleAccessor(value));
            return this;
        }

        public UpdateBuilder update(StringField field, StringAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(StringField field, String value) {
            fieldsValues.put(field, new ValueStringAccessor(value));
            return this;
        }

        public UpdateBuilder update(BooleanField field, BooleanAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public UpdateBuilder update(BooleanField field, Boolean value) {
            fieldsValues.put(field, new ValueBooleanAccessor(value));
            return this;
        }

        public UpdateBuilder update(BlobField field, byte[] value) {
            fieldsValues.put(field, new ValueBlobAccessor(value));
            return this;
        }

        public UpdateBuilder update(BlobField field, BlobAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public SqlRequest getRequest() {
            return new MongoSqlRequest(sqlService, mongoDatabase, globType, constraint, fieldsValues);
        }

        private static class MongoSqlRequest implements SqlRequest {
            private final MongoCollection<Document> collection;
            private SqlService sqlService;
            private final MongoDatabase mongoDatabase;
            private final GlobType globType;
            private final Constraint constraint;
            private final Map<Field, Accessor> fieldsValues;

            public MongoSqlRequest(SqlService sqlService, MongoDatabase mongoDatabase, GlobType globType, Constraint constraint, Map<Field, Accessor> fieldsValues) {
                this.sqlService = sqlService;
                this.mongoDatabase = mongoDatabase;
                this.globType = globType;
                this.constraint = constraint;
                this.fieldsValues = fieldsValues;
                collection = mongoDatabase.getCollection(sqlService.getTableName(globType));
            }

            public void run() throws SqlException {
                Bson filter;
                if (constraint != null) {
                    MongoConstraintVisitor constraintVisitor = new MongoConstraintVisitor(sqlService);
                    constraint.visit(constraintVisitor);
                    filter = constraintVisitor.filter;
                } else {
                    filter = new Document();
                }
                Bson combine = Updates.combine(fieldsValues.entrySet().stream().map(entry -> {
                    Object objectValue = entry.getValue().getObjectValue();
                    Field field = entry.getKey();
                    if (field.hasAnnotation(DbRef.KEY)) {
                        String type = field.getAnnotation(DbRef.KEY).get(DbRef.TO);
                        Document document = new Document();
                        document.append(MongoUtils.DB_REF_ID_EXT, new ObjectId((String) objectValue));
                        document.append(MongoUtils.DB_REF_REF_EXT, type);
                        return Updates.set(MongoUtils.getDbName(field), document);
                    } else {
                        return Updates.set(sqlService.getColumnName(field), objectValue);
                    }

                }).collect(Collectors.toList()));
                collection.updateOne(filter, combine);
            }

            public void close() {

            }
        }
    }
}
