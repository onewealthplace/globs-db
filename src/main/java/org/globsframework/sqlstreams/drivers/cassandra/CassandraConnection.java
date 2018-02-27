package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.cassandra.impl.CassandraFieldCreationVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class CassandraConnection implements SqlConnection {
    private Session session;
    private DbCasandra sqlService;

    public CassandraConnection(Session session, DbCasandra sqlService) {
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
        return new CassandraCreateBuilder(session, globType, sqlService);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        return new CassandraUpdateBuilder(session, globType, sqlService, constraint);
    }

    public SqlRequest getDeleteRequest(GlobType globType) {
        throw new RuntimeException("delete request");
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        throw new RuntimeException("delete request");
    }

    public void commit() throws RollbackFailed, DbConstraintViolation {
    }

    public void commitAndClose() throws RollbackFailed, DbConstraintViolation {
    }

    public void rollbackAndClose() {
    }

    public void createTable(GlobType... globType) {
        Arrays.stream(globType).forEach(this::createTable);
    }

    private void createTable(GlobType globType) {
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("CREATE TABLE IF NOT EXISTS ")
              .append(sqlService.getKeySpace())
              .append(".")
              .append(sqlService.getTableName(globType))
              .append(" ( ");
        CassandraFieldCreationVisitor creationVisitor = new CassandraFieldCreationVisitor(sqlService, writer);
        int count = 1;
        for (Field field : globType.getFields()) {
            field.safeVisit(creationVisitor.appendComma(count != globType.getFieldCount()));
            count++;
        }
        Field[] keyFields = globType.getKeyFields();
        if (keyFields.length != 0) {
            Field last = keyFields[keyFields.length - 1];
            writer.append(", PRIMARY KEY (");
            for (Field field : keyFields) {
                writer.append(sqlService.getColumnName(field))
                      .appendIf(", ", last != field);
            }
            writer.append(") ");
        }
        writer.append(");");
        session.execute(writer.toString());
    }

    public void emptyTable(GlobType... globType) {
    }

    public void showDb() {
    }

    public void populate(GlobList all) {
        for (Glob glob : all) {
            CreateBuilder createBuilder = getCreateBuilder(glob.getType());
            for (Field field : glob.getType().getFields()) {
                createBuilder.setObject(field, glob.getValue(field));
            }
            createBuilder.getRequest().run();
        }
    }

}
