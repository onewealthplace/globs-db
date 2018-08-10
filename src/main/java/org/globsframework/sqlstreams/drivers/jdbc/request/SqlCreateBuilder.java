package org.globsframework.sqlstreams.drivers.jdbc.request;

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
import org.globsframework.utils.exceptions.NotSupported;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlCreateBuilder implements CreateBuilder {
    private Connection connection;
    private GlobType globType;
    private SqlService sqlService;
    private BlobUpdater blobUpdater;
    private JdbcConnection jdbcConnection;
    private List<Pair<Field, Accessor>> fields = new ArrayList<Pair<Field, Accessor>>();
    private Set<Field> fieldSet = new HashSet<>();
    protected LongGeneratedKeyAccessor longGeneratedKeyAccessor;

    public SqlCreateBuilder(Connection connection, GlobType globType, SqlService sqlService,
                            BlobUpdater blobUpdater, JdbcConnection jdbcConnection) {
        this.connection = connection;
        this.globType = globType;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
        this.jdbcConnection = jdbcConnection;
    }

    public CreateBuilder setObject(Field field, Accessor accessor) {
        fields.add(new Pair<>(field, accessor));
        if (!fieldSet.add(field)) {
            throw new RuntimeException("Field already registered");
        }
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

            @Override
            public void visitArray(ArrayField field) throws Exception {
                throw new NotSupported("TODO: remove") ;
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

    public LongAccessor getKeyGeneratedAccessor() {
        if (longGeneratedKeyAccessor == null) {
            longGeneratedKeyAccessor = new LongGeneratedKeyAccessor();
        }
        return longGeneratedKeyAccessor;
    }

    public SqlRequest getRequest() {
        return new SqlCreateRequest(fields, longGeneratedKeyAccessor, connection, globType, sqlService, blobUpdater, jdbcConnection);
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
