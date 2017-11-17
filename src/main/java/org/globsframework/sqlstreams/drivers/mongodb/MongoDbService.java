package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoDatabase;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.utils.AbstractSqlService;

public class MongoDbService extends AbstractSqlService {
    private final MongoDatabase database;

    public MongoDbService(MongoDatabase database) {
        this.database = database;
    }

    public SqlConnection getDb() {
        return new MangoDbConnection(database, this);
    }
}
