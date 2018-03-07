package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.cassandra.impl.ValueConstraintVisitor;
import org.globsframework.sqlstreams.drivers.cassandra.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.AccessorGlobBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.AccessorGlobsBuilder;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.streams.GlobStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public class CassandraSelectQuery implements SelectQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSelectQuery.class);
    private final Session session;
    private Set<GlobType> globTypes = new HashSet<GlobType>();
    private Constraint constraint;
    private boolean autoClose;
    private Map<Field, CasAccessor> fieldToAccessorHolder;
    private DbCasandra sqlService;
    private final List<Order> orders;
    private final int top;
    private Set<Field> distinct;
    private PreparedStatement preparedStatement;
    private String sql;


    public static class Order {
        public final Field field;
        public final boolean asc;

        public Order(Field field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }
    }

    public CassandraSelectQuery(Session session, Constraint constraint,
                                Map<Field, CasAccessor> fieldToAccessorHolder, DbCasandra sqlService,
                                boolean autoClose, List<Order> orders, int top, Set<Field> distinct) {
        this.constraint = constraint;
        this.autoClose = autoClose;
        this.fieldToAccessorHolder = new HashMap<>(fieldToAccessorHolder);
        this.sqlService = sqlService;
        this.orders = orders;
        this.top = top;
        this.distinct = distinct;
        sql = prepareSqlRequest();
        this.session = session;
        LOGGER.info("call " + sql);
        preparedStatement = session.prepare(sql);
    }

    private String prepareSqlRequest() {
        int index = 0;
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("select ");
        for (Iterator<Map.Entry<Field, CasAccessor>> iterator = fieldToAccessorHolder.entrySet().iterator();
             iterator.hasNext(); ) {
            Map.Entry<Field, CasAccessor> fieldAndAccessor = iterator.next();
            fieldAndAccessor.getValue().setIndex(index++);
            GlobType globType = fieldAndAccessor.getKey().getGlobType();
            globTypes.add(globType);
            if (distinct.contains(fieldAndAccessor.getKey())) {
                prettyWriter.append(" DISTINCT ");
            }
            prettyWriter
                  .append(sqlService.getColumnName(fieldAndAccessor.getKey()))
                  .appendIf(", ", iterator.hasNext());
        }
        StringPrettyWriter where = null;
        if (constraint != null) {
            where = new StringPrettyWriter();
            where.append(" WHERE ");
            constraint.visit(new WhereClauseConstraintVisitor(where, sqlService, globTypes));
        }

        prettyWriter.append(" from ");
        for (Iterator it = globTypes.iterator(); it.hasNext(); ) {
            GlobType globType = (GlobType) it.next();
            prettyWriter
                  .append(sqlService.getKeySpace())
                  .append(".")
                  .append(sqlService.getTableName(globType))
                  .appendIf(", ", it.hasNext());
        }
        if (where != null) {
            prettyWriter.append(where.toString());
        }

        if (!orders.isEmpty()) {
            prettyWriter.append(" ORDER BY ");
            for (Order order : orders) {
                prettyWriter.append(sqlService.getColumnName(order.field));
                if (order.asc) {
                    prettyWriter.append(" ASC");
                } else {
                    prettyWriter.append(" DESC");
                }
                prettyWriter.append(", ");
            }
            prettyWriter.removeLast().removeLast();
        }
        prettyWriter.append(" LIMIT " + (top == -1 ? Integer.MAX_VALUE : top));
//        prettyWriter.append(" ALLOW FILTERING ");
        prettyWriter.append(";");
        return prettyWriter.toString();
    }

    public Stream<?> executeAsStream() {
        throw new RuntimeException("Not implemented");
    }

    public GlobStream execute() {
        if (preparedStatement == null) {
            throw new UnexpectedApplicationState("Query closed " + sql);
        }
        BoundStatement bind = preparedStatement.bind();
        if (constraint != null) {
            constraint.visit(new ValueConstraintVisitor(bind));
        }
        return new CassandraGlobStream(session.execute(bind), fieldToAccessorHolder, this);
    }

    public GlobList executeAsGlobs() {
        GlobStream globStream = execute();
        AccessorGlobBuilder accessorGlobsBuilder = AccessorGlobBuilder.init(globStream);
        GlobList result = new GlobList();
        while (globStream.next()) {
            result.add(accessorGlobsBuilder.getGlob());
        }
        return result;
    }

    public Glob executeUnique() throws ItemNotFound, TooManyItems {
        GlobList globs = executeAsGlobs();
        if (globs.size() == 1) {
            return globs.get(0);
        }
        if (globs.isEmpty()) {
            throw new ItemNotFound("No result returned for: " + sql);
        }
        throw new TooManyItems("Too many results for: " + sql);
    }

    public void resultSetClose() {
        if (autoClose) {
            close();
        }
    }

    public void close() {
        if (preparedStatement != null) {
//            preparedStatement.close();
            preparedStatement = null;
        }
    }
}
