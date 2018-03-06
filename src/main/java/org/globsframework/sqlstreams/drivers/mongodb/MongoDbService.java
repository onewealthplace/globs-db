package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.annotations.IsDbKey;
import org.globsframework.sqlstreams.utils.AbstractSqlService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MongoDbService extends AbstractSqlService {
    private final MongoDatabase database;
    private final Executor executor = Executors.newCachedThreadPool();
    private final UpdateAdapterFactory updateAdapterFactory;
    private ConcurrentHashMap<Field, UpdateAdapter> adapter = new ConcurrentHashMap<>();

    public MongoDbService(MongoDatabase database) {
        this(database, new DefaultUpdateAdapterFactory());
    }

    public MongoDbService(MongoDatabase database, UpdateAdapterFactory updateAdapterFactory) {
        this.database = database;
        this.updateAdapterFactory = updateAdapterFactory;
    }


    public SqlConnection getDb() {
        return new MongoDbConnection(database, this);
    }

    public String getColumnName(Field field) {
        return MongoUtils.getFullDbName(field);
    }

    public String getFirstLevelColumnName(Field field) {
        return MongoUtils.getDbName(field);
    }

    public Executor getExecutor() {
        return executor;
    }

    public UpdateAdapter getAdapter(Field field) {
        return adapter.computeIfAbsent(field, updateAdapterFactory::get);
    }

    interface UpdateAdapterFactory {
        UpdateAdapter get(Field field);
    }

    static public class DefaultUpdateAdapterFactory implements UpdateAdapterFactory {

        public UpdateAdapter get(Field field) {
            if (field.hasAnnotation(IsDbKey.KEY)) {
                return new IdUpdateAdapter(field);
            }else if (field.hasAnnotation(DbRef.KEY)) {
                return new DefaultRefKeyUpdater(field);
            } else {
                return new DefaultUpdater(field);
            }
        }

        private static class IdUpdateAdapter implements UpdateAdapter {
            private final String name;
            private final Field field;

            public IdUpdateAdapter(Field field) {
                name = MongoUtils.getDbName(field);
                this.field = field;
            }

            public void create(Object value, Document document) {
                document.put(name, new ObjectId((String) value));
            }

            public Bson update(Object value) {
                throw new RuntimeException("Call to update on id filed not expected for " + field.getFullName());
            }
        }
    }


    public interface UpdateAdapter {
        void create(Object value, Document document);

        Bson update(Object value);
    }

    static class DefaultUpdater implements UpdateAdapter {
        private final String name;

        public DefaultUpdater(Field field) {
            name = MongoUtils.getDbName(field);
        }

        public void create(Object value, Document doc) {
            doc.append(name, value);
        }

        public Bson update(Object value) {
            return Updates.set(name, value);
        }
    }

    static class DefaultRefKeyUpdater implements UpdateAdapter {
        private final String type;
        private final String name;

        DefaultRefKeyUpdater(Field field) {
            type = field.getAnnotation(DbRef.KEY).get(DbRef.TO);
            name = MongoUtils.getDbName(field);
        }

        public void create(Object value, Document doc) {
            Document document = new Document();
            document.append(MongoUtils.DB_REF_ID_EXT, new ObjectId((String) value));
            document.append(MongoUtils.DB_REF_REF_EXT, type);
            doc.append(name, document);
        }

        public Bson update(Object value) {
            Document document = new Document();
            document.append(MongoUtils.DB_REF_ID_EXT, new ObjectId((String) value));
            document.append(MongoUtils.DB_REF_REF_EXT, type);
            return Updates.set(name, document);
        }
    }
}
