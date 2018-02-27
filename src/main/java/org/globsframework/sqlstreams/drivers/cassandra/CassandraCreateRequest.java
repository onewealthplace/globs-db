package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.GeneratedKeyAccessor;
import org.globsframework.sqlstreams.drivers.cassandra.impl.SqlValueFieldVisitor;
import org.globsframework.sqlstreams.utils.PrettyWriter;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.collections.Pair;

import java.util.Iterator;
import java.util.List;

public class CassandraCreateRequest implements SqlRequest {
    private final Session session;
    private PreparedStatement preparedStatement;
    private List<Pair<Field, Accessor>> fields;
    private GeneratedKeyAccessor generatedKeyAccessor;
    private GlobType globType;
    private SqlService sqlService;

    public CassandraCreateRequest(List<Pair<Field, Accessor>> fields, GeneratedKeyAccessor generatedKeyAccessor,
                                  Session session,
                                  GlobType globType, SqlService sqlService) {
        this.generatedKeyAccessor = generatedKeyAccessor;
        this.fields = fields;
        this.globType = globType;
        this.sqlService = sqlService;
        String sql = prepareRequest(fields, this.globType, new Value() {
            public String get(Pair<Field, Accessor> pair) {
                return "?";
            }
        });
        this.session = session;
        preparedStatement = session.prepare(sql);
    }

    interface Value {
        String get(Pair<Field, Accessor> pair);
    }

    private String prepareRequest(List<Pair<Field, Accessor>> fields, GlobType globType, Value value) {
        PrettyWriter writer = new StringPrettyWriter();
        writer.append("INSERT INTO ")
              .append(sqlService.getTableName(globType))
              .append(" (");
        int columnCount = 0;
        for (Pair<Field, Accessor> pair : fields) {
            String columnName = sqlService.getColumnName(pair.getFirst());
            writer.appendIf(", ", columnCount > 0);
            columnCount++;
            writer.append(columnName);
        }
        writer.append(") VALUES (");
        for (Iterator<Pair<Field, Accessor>> it = fields.iterator(); it.hasNext(); ) {
            Pair<Field, Accessor> pair = it.next();
            writer.append(value.get(pair)).appendIf(",", it.hasNext());
        }
        writer.append(")");
        return writer.toString();
    }

    public void run() {
        int index = 0;
        BoundStatement boundStatement = preparedStatement.bind();
        SqlValueFieldVisitor sqlValueVisitor = new SqlValueFieldVisitor(boundStatement);
        for (Pair<Field, Accessor> pair : fields) {
            Object value = pair.getSecond().getObjectValue();
            sqlValueVisitor.setValue(value, ++index);
            pair.getFirst().safeVisit(sqlValueVisitor);
        }
        session.execute(boundStatement);
    }

    public void close() {
    }

    private String getDebugRequest() {
        return prepareRequest(fields, globType, new DebugValue());
    }

    private static class DebugValue implements Value, FieldVisitor {
        private Object value;
        private String convertValue;

        public String get(Pair<Field, Accessor> pair) {
            value = pair.getSecond().getObjectValue();
            if (value != null) {
                pair.getFirst().safeVisit(this);
            } else {
                convertValue = "'NULL'";
            }
            return convertValue;
        }

        public void visitInteger(IntegerField field) throws Exception {
            convertValue = value.toString();
        }

        public void visitDouble(DoubleField field) throws Exception {
            convertValue = value.toString();
        }

        public void visitString(StringField field) throws Exception {
            convertValue = "'" + value.toString() + "'";
        }

        public void visitBoolean(BooleanField field) throws Exception {
            convertValue = value.toString();
        }

        public void visitBlob(BlobField field) throws Exception {
            convertValue = "'" + value.toString() + "'";
        }

        public void visitLong(LongField field) throws Exception {
            convertValue = value.toString();
        }

    }
}
