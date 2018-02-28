package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoDatabase;
import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.utils.AbstractSqlService;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MongoDbService extends AbstractSqlService {
    private final MongoDatabase database;
    private final Executor executor = Executors.newCachedThreadPool();

    public MongoDbService(MongoDatabase database) {
        this.database = database;
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
}
