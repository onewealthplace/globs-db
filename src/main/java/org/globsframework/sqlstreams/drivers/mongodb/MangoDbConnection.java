package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoDatabase;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;

import java.sql.Connection;

public class MangoDbConnection implements SqlConnection {
    MongoDatabase mongoDatabase;
    SqlService sqlService;

    public MangoDbConnection(MongoDatabase mongoDatabase, SqlService sqlService) {
        this.mongoDatabase = mongoDatabase;
        this.sqlService = sqlService;
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        return new MongoSelectBuilder(mongoDatabase, globType, sqlService);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        return null;
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
}
