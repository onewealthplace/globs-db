package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.GlobTypeBuilderFactory;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlCreateBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlDeleteBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlUpdateBuilder;
import org.globsframework.sqlstreams.exceptions.ConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.sqlstreams.metadata.DbChecker;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.utils.exceptions.GlobsException;
import org.globsframework.utils.exceptions.OperationDenied;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public abstract class JdbcConnection implements SqlConnection {
    private Logger LOGGER  = LoggerFactory.getLogger(JdbcConnection.class);
    private Connection connection;
    protected SqlService sqlService;
    private BlobUpdater blobUpdater;
    private DbChecker checker;

    public JdbcConnection(Connection connection, SqlService sqlService, BlobUpdater blobUpdater) {
        this.connection = connection;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
        checker = new DbChecker(sqlService, this);
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, null, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, constraint, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, String sqlRequest) {
        checkConnectionIsNotClosed();
//            PreparedStatement preparedStatement = connection.prepareStatement(sqlRequest);
//            ResultSetMetaData metaData = preparedStatement.getMetaData();
//            GlobTypeBuilder globTypeBuilder = GlobTypeBuilderFactory.create("SQL_REQUEST");
//            initColumns(globTypeBuilder, metaData);
//            GlobType globType = globTypeBuilder.get();
//            LOGGER.info("GlobType deduce from '" + sqlRequest + " => " + globType.describe());
        return new SqlQueryBuilder(connection, globType, null, sqlService, blobUpdater, sqlRequest);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlUpdateBuilder(connection, globType, sqlService, constraint, blobUpdater);
    }

    private void checkConnectionIsNotClosed() {
        if (connection == null) {
            throw new UnexpectedApplicationState("connection was closed");
        }
    }

    interface DbFunctor {
        void doIt() throws SQLException;
    }

    public void commit() throws RollbackFailed {
        checkConnectionIsNotClosed();
        try {
            connection.commit();
        } catch (SQLException e) {
            throw getTypedException(null, e);
        }
    }

    public void commitAndClose() {
        applyAndClose(new DbFunctor() {
            public void doIt() throws SQLException {
                connection.commit();
            }
        });
    }

    public void rollbackAndClose() {
        applyAndClose(new DbFunctor() {
            public void doIt() throws SQLException {
                connection.rollback();
            }
        });
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return new SqlCreateBuilder(connection, globType, sqlService, blobUpdater, this);
    }

    public void createTable(GlobType... globTypes) {
        for (GlobType type : globTypes) {
            createTable(type);
        }
    }

    private void createTable(GlobType globType) {
        if (checker.tableExists(globType)) {
            return;
        }
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("CREATE TABLE ")
              .append(sqlService.getTableName(globType))
              .append(" ( ");
        SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);
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
        try {
            PreparedStatement statement = connection.prepareStatement(writer.toString());
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("Invalid creation request: " + writer.toString(), e);
        }
    }

    public void emptyTable(GlobType... globTypes) {
        for (GlobType globType : globTypes) {
            emptyTable(globType);
        }
    }

    private void emptyTable(GlobType globType) {
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("DELETE FROM ")
              .append(sqlService.getTableName(globType))
              .append(";");
        try {
            PreparedStatement statament = connection.prepareStatement(writer.toString());
            statament.executeUpdate();
            statament.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("Unable to empty table : " + writer.toString(), e);
        }
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

    abstract protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter);

    public SqlRequest getDeleteRequest(GlobType globType) {
        return new SqlDeleteBuilder(globType, null, connection, sqlService, blobUpdater);
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        return new SqlDeleteBuilder(globType, constraint, connection, sqlService, blobUpdater);
    }

    public Connection getConnection() {
        return connection;
    }

    public SqlException getTypedException(String sql, SQLException e) {
        if ("23000".equals(e.getSQLState())) {
            if (sql == null) {
                return new ConstraintViolation(e);
            } else {
                return new ConstraintViolation(sql, e);
            }
        }
        return new SqlException(e);
    }

    private void applyAndClose(DbFunctor db) {
        if (connection == null) {
            return;
        }
        GlobsException ex = null;
        try {
            db.doIt();
        } catch (SQLException e) {
            ex = getTypedException(null, e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                if (ex == null) {
                    ex = new OperationDenied(e);
                }
            } finally {
                connection = null;
            }
            if (ex != null) {
                throw ex;
            }
        }
    }
    private void initColumns(GlobTypeBuilder typeBuilder, ResultSetMetaData resultSetMetaData) throws SQLException {
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 1 ; i <= columnCount; i++)
        {
                String fieldName = resultSetMetaData.getColumnName(i);
                int dataType = resultSetMetaData.getColumnType(i);
                switch (dataType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR: {
                        typeBuilder.declareStringField(fieldName);
                    }
                    case Types.DECIMAL:
                    case Types.NUMERIC: {
                        int precision = resultSetMetaData.getPrecision(i);
                        int scale = resultSetMetaData.getScale(i);
                        if (scale == 0 && precision < 9) {
                            typeBuilder.declareIntegerField(fieldName);
                        }
                        else if (scale == 0 && precision < 18) {
                            typeBuilder.declareLongField(fieldName);
                        }
                        else {
                            typeBuilder.declareDoubleField(fieldName);
                        }
                        break;
                    }
                    case Types.FLOAT:
                    case Types.DOUBLE: {
                        typeBuilder.declareDoubleField(fieldName);
                        break;
                    }
                    case Types.BIT:
                    case Types.BOOLEAN:{
                        Field field = typeBuilder.declareBooleanField(fieldName);
                    }
                    break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER: {
                        typeBuilder.declareIntegerField(fieldName);
                    }
                    break;
                    case Types.BIGINT: {
                        typeBuilder.declareLongField(fieldName);
                    }
                    break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:{
                        Field field = typeBuilder.declareBlobField(fieldName);
                        break;
                    }
                    case Types.DATE:{
                        Field field = typeBuilder.declareIntegerField(fieldName);
                        break;
                    }
                    case Types.TIMESTAMP:
                    case Types.OTHER:
                        LOGGER.warn(fieldName + " is of type 'other' => ignored");
                        break;
                    default:
                        throw new GlobsException("Type " + getNameForType(dataType) + " not managed");
                }
            }
    }

    private String getNameForType(int dataType) {
        switch (dataType) {
            case Types.CHAR:
                return "CHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.FLOAT:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.BIT:
                return "BIT";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.BINARY:
                return "BINARY";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.BLOB:
                return "BLOB";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.STRUCT:
                return "STRUCT";
            case Types.ARRAY:
                return "ARRAY";
            case Types.CLOB:
                return "CLOB";
            case Types.JAVA_OBJECT:
                return "JAVA_OBJECT";
            default:
                return "Unknwon : " + dataType;
        }
    }
}
