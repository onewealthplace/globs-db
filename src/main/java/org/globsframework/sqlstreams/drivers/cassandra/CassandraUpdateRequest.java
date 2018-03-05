package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Key;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.cassandra.impl.CassandraValueFieldVisitor;
import org.globsframework.sqlstreams.drivers.cassandra.impl.ValueConstraintVisitor;
import org.globsframework.sqlstreams.drivers.cassandra.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraUpdateRequest implements SqlRequest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraUpdateRequest.class);
    private GlobType globType;
    private Constraint constraint;
    private Map<Field, Accessor> values;
    private Session session;
    private DbCasandra sqlService;
    private com.datastax.driver.core.PreparedStatement preparedStatement;
    private String sqlRequest;
    private int count;

    public CassandraUpdateRequest(GlobType globType, Constraint constraint, Map<Field, Accessor> values,
                                  Session session, DbCasandra sqlService) {
        this.globType = globType;
        this.constraint = constraint;
        this.values = new HashMap<>(values);
        this.session = session;
        this.sqlService = sqlService;
        sqlRequest = createRequest();
        LOGGER.info("update request : " + sqlRequest);
        preparedStatement = session.prepare(sqlRequest);
    }

    public void run() {
        count++;
        BoundStatement boundStatement = preparedStatement.bind();
        CassandraValueFieldVisitor cassandraValueFieldVisitor = new CassandraValueFieldVisitor(boundStatement);
        int index = 0;
        for (Map.Entry<Field, Accessor> entry : values.entrySet()) {
            cassandraValueFieldVisitor.setValue(entry.getValue().getObjectValue(), index++);
            entry.getKey().safeVisit(cassandraValueFieldVisitor);
        }
        constraint.visit(new ValueConstraintVisitor(boundStatement, index));
        session.execute(boundStatement);
    }

    public void close() {
        LOGGER.info(count + " update done");
    }

    public void execute(Key key) {
        GlobType globType = key.getGlobType();
        Field[] list = globType.getKeyFields();
        Constraint constraint = null;
        for (Field field : list) {
            constraint = Constraints.and(constraint, Constraints.equalsObject(field, key.getValue(field)));
        }
        this.constraint = Constraints.and(this.constraint, constraint);
        run();
    }

    private String createRequest() {
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("UPDATE ")
              .append(sqlService.getKeySpace())
              .append(".")
              .append(sqlService.getTableName(globType))
              .append(" SET ");
        for (Iterator it = values.keySet().iterator(); it.hasNext(); ) {
            Field field = (Field) it.next();
            prettyWriter
                  .append(sqlService.getColumnName(field))
                  .append(" = ?").
                  appendIf(" , ", it.hasNext());
        }
        prettyWriter.append(" WHERE ");
        Set<GlobType> globTypes = new HashSet<GlobType>();
        globTypes.add(globType);
        constraint.visit(new WhereClauseConstraintVisitor(prettyWriter, sqlService, globTypes));
        if (globTypes.size() > 1) {
            throw new UnexpectedApplicationState("Only the updated table is valide in query " + prettyWriter.toString());
        }

        prettyWriter.append(";");
        return prettyWriter.toString();
    }
}
