package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
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

import java.util.ArrayList;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
        throw new RuntimeException("Not Implemented");
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

    private static class MongoCreateBuilder implements CreateBuilder {
        private final MongoDatabase mongoDatabase;
        private final GlobType globType;
        private final MongoDbService sqlService;
        Map<Field, Accessor> fieldsValues = new HashMap<>();

        public MongoCreateBuilder(MongoDatabase mongoDatabase, GlobType globType, MongoDbService sqlService) {
            this.mongoDatabase = mongoDatabase;
            this.globType = globType;
            this.sqlService = sqlService;
        }

        public CreateBuilder set(IntegerField field, Integer value) {
            fieldsValues.put(field, new ValueIntegerAccessor(value));
            return this;
        }

        public CreateBuilder set(BlobField field, byte[] value) {
            fieldsValues.put(field, new ValueBlobAccessor(value));
            return this;
        }

        public CreateBuilder set(StringField field, String value) {
            fieldsValues.put(field, new ValueStringAccessor(value));
            return this;
        }

        public CreateBuilder set(DoubleField field, Double value) {
            fieldsValues.put(field, new ValueDoubleAccessor(value));
            return this;
        }

        public CreateBuilder set(BooleanField field, Boolean value) {
            fieldsValues.put(field, new ValueBooleanAccessor(value));
            return this;
        }

        public CreateBuilder set(LongField field, Long value) {
            fieldsValues.put(field, new ValueLongAccessor(value));
            return this;
        }

        public CreateBuilder set(IntegerField field, IntegerAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder set(LongField field, LongAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder set(StringField field, StringAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder set(DoubleField field, DoubleAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder set(BooleanField field, BooleanAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder set(BlobField field, BlobAccessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder setObject(Field field, Accessor accessor) {
            fieldsValues.put(field, accessor);
            return this;
        }

        public CreateBuilder setObject(Field field, Object value) {
            field.safeVisit(new FieldValueVisitor() {
                public void visitInteger(IntegerField field, Integer value) throws Exception {
                    set(field, value);
                }

                public void visitDouble(DoubleField field, Double value) throws Exception {
                    set(field, value);
                }

                public void visitString(StringField field, String value) throws Exception {
                    set(field, value);
                }

                public void visitBoolean(BooleanField field, Boolean value) throws Exception {
                    set(field, value);
                }

                public void visitLong(LongField field, Long value) throws Exception {
                    set(field, value);
                }

                public void visitBlob(BlobField field, byte[] value) throws Exception {
                    set(field, value);
                }
            }, value);
            return this;
        }

        public SqlRequest getRequest() {
            return new MongoCreateSqlRequest(mongoDatabase.getCollection(sqlService.getTableName(globType)), fieldsValues, sqlService, false);
        }

        public BulkDbRequest getBulkRequest() {
            return new MongoCreateSqlRequest(mongoDatabase.getCollection(sqlService.getTableName(globType)), fieldsValues, sqlService, true);
        }

        private static class MongoCreateSqlRequest implements BulkDbRequest {
            private MongoCollection<Document> collection;
            private Map<Field, Accessor> fieldsValues;
            private MongoDbService sqlService;
            private boolean bulk;
            private List<Document> docs;
            CompletableFuture<Boolean> completableFuture;
            private int count = 0;

            public MongoCreateSqlRequest(MongoCollection<Document> collection,
                                         Map<Field, Accessor> fieldsValues, MongoDbService sqlService, boolean bulk) {
                this.collection = collection;
                this.fieldsValues = fieldsValues;
                this.sqlService = sqlService;
                this.bulk = bulk;
            }

            public void run() throws SqlException {
                Document doc = new Document();
                for (Map.Entry<Field, Accessor> fieldAccessorEntry : fieldsValues.entrySet()) {
                    Object objectValue = fieldAccessorEntry.getValue().getObjectValue();
                    if (objectValue != null) {
                        if (fieldAccessorEntry.getKey().hasAnnotation(DbRef.KEY)) {
                            Document document = new Document();
                            document.append(MongoUtils.DB_REF_ID_EXT, new ObjectId((String) objectValue));
                            doc.append(MongoUtils.getDbName(fieldAccessorEntry.getKey()), document);
                        } else {
                            doc.append(MongoUtils.getFullDbName(fieldAccessorEntry.getKey()), objectValue);
                        }
                    }
                }

                if (++count <= 2 || bulk) {
                    collection.insertOne(doc);
                } else {
                    if (docs == null) {
                        docs = new ArrayList<>(100);
                    }
                    docs.add(doc);
                    if (docs.size() == 100) {
                        if (completableFuture != null) {
                            if (completableFuture.isCompletedExceptionally()) {
                                try {
                                    completableFuture.get();
                                } catch (Exception e) {
                                    throw new RuntimeException("Create failed ", e);
                                }
                            }
                            final List<Document> toInsert = docs;
                            docs = null;
                            completableFuture = completableFuture.
                                  thenApply(ok -> {
                                      collection.insertMany(toInsert);
                                      return true;
                                  });
                        } else {
                            final List<Document> toInsert = docs;
                            docs = null;
                            completableFuture = CompletableFuture.supplyAsync(() -> {
                                collection.insertMany(toInsert);
                                return Boolean.TRUE;
                            }, sqlService.getExecutor());
                        }
                    }
                }
            }

            public void close() {
                if (completableFuture != null) {
                    if (docs != null && !docs.isEmpty()) {
                        completableFuture = completableFuture.thenApply(ok -> {
                            collection.insertMany(docs);
                            return true;
                        });
                    }
                    try {
                        completableFuture.get(1, TimeUnit.MINUTES);
                        completableFuture = null;
                    } catch (Exception e) {
                        completableFuture = null;
                        throw new RuntimeException("In close, fail to insert all data", e);
                    }
                }
                else {
                    if (docs != null && !docs.isEmpty()) {
                        collection.insertMany(docs);
                    }
                }
            }

            public void flush() {
                close();
            }
        }
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
}
