package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.LongGeneratedKeyAccessor;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.SqlCreateRequest;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.streams.accessors.*;
import org.globsframework.streams.accessors.utils.*;
import org.globsframework.utils.collections.Pair;

import java.util.ArrayList;
import java.util.List;

public class CassandraCreateBuilder implements CreateBuilder {
    private Session session;
    private GlobType globType;
    private DbCasandra sqlService;
    private BlobUpdater blobUpdater;
    private JdbcConnection jdbcConnection;
    private List<Pair<Field, Accessor>> fields = new ArrayList<Pair<Field, Accessor>>();
    protected LongGeneratedKeyAccessor longGeneratedKeyAccessor;

    public CassandraCreateBuilder(Session session, GlobType globType, DbCasandra sqlService) {
        this.session = session;
        this.globType = globType;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
        this.jdbcConnection = jdbcConnection;
    }

    public CreateBuilder setObject(Field field, Accessor accessor) {
        fields.add(new Pair<Field, Accessor>(field, accessor));
        return this;
    }

    public CreateBuilder setObject(Field field, final Object value) {
        field.safeVisit(new FieldVisitor() {
            public void visitInteger(IntegerField field) {
                setObject(field, new ValueIntegerAccessor((Integer) value));
            }

            public void visitLong(LongField field) {
                setObject(field, new ValueLongAccessor((Long) value));
            }

            public void visitDouble(DoubleField field) {
                setObject(field, new ValueDoubleAccessor((Double) value));
            }

            public void visitString(StringField field) {
                setObject(field, new ValueStringAccessor((String) value));
            }

            public void visitBoolean(BooleanField field) {
                setObject(field, new ValueBooleanAccessor((Boolean) value));
            }

            public void visitBlob(BlobField field) {
                setObject(field, new ValueBlobAccessor((byte[]) value));
            }

        });
        return this;
    }

    public CreateBuilder set(IntegerField field, IntegerAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(LongField field, LongAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(StringField field, StringAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(DoubleField field, DoubleAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BooleanField field, BooleanAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BlobField field, BlobAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BlobField field, byte[] values) {
        return setObject(field, new ValueBlobAccessor(values));
    }

    public CreateBuilder set(StringField field, String value) {
        return setObject(field, new ValueStringAccessor(value));
    }

    public CreateBuilder set(LongField field, Long value) {
        return setObject(field, new ValueLongAccessor(value));
    }

    public CreateBuilder set(DoubleField field, Double value) {
        return setObject(field, new ValueDoubleAccessor(value));
    }

    public CreateBuilder set(BooleanField field, Boolean value) {
        return setObject(field, new ValueBooleanAccessor(value));
    }

    public CreateBuilder set(IntegerField field, Integer value) {
        return setObject(field, new ValueIntegerAccessor(value));
    }

    public SqlRequest getRequest() {
        return new CassandraCreateRequest(fields, longGeneratedKeyAccessor, session, globType, sqlService);
    }

    public BulkDbRequest getBulkRequest() {
        SqlRequest request = getRequest();
        return new BulkDbRequest() {
            public void flush() {
            }

            public void run() throws SqlException {
                request.run();
            }

            public void close() {
                request.close();
            }
        };
    }
}
