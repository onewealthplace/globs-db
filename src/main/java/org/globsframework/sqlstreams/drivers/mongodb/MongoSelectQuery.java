package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.sqlstreams.drivers.jdbc.AccessorGlobsBuilder;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.Ref;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.include;

public class MongoSelectQuery implements SelectQuery {
    public static final Logger LOGGER = LoggerFactory.getLogger(MongoSelectQuery.class);
    private final MongoCollection<Document> collection;
    private final Map<Field, Accessor> fieldsAndAccessor;
    private final Ref<Document> currentDoc;
    private final GlobType globType;
    private final String request;
    private String lastFullRequest;
    private SqlService sqlService;
    private Constraint constraint;
    private final List<MongoSelectBuilder.Order> orders;
    private final int top;

    public MongoSelectQuery(MongoCollection<Document> collection, Map<Field, Accessor> fieldsAndAccessor,
                            Ref<Document> currentDoc, GlobType globType, SqlService sqlService, Constraint constraint,
                            List<MongoSelectBuilder.Order> orders, int top) {
        this.collection = collection;
        this.fieldsAndAccessor = fieldsAndAccessor;
        this.currentDoc = currentDoc;
        this.globType = globType;
        request = "select on " + globType.getName();
        this.sqlService = sqlService;
        this.constraint = constraint;
        this.orders = orders;
        this.top = top;
    }

    public Stream<?> executeAsStream() {
        DocumentsIterator iterator = getDocumentsIterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    private DocumentsIterator getDocumentsIterator() {
        Bson filter;
        if (constraint != null) {
            MongoConstraintVisitor constraintVisitor = new MongoConstraintVisitor(sqlService);
            constraint.visit(constraintVisitor);
            filter = constraintVisitor.filter;
        } else {
            filter = new Document();
        }

        lastFullRequest = request + " where " + filter.toBsonDocument(BsonDocument.class, collection.getCodecRegistry());

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Request filter : " + lastFullRequest);
        }
        Bson include = include(fieldsAndAccessor.keySet()
              .stream()
              .map(sqlService::getColumnName).collect(Collectors.toList()));
        FindIterable<Document> findIterable = collection.find()
              .filter(filter);
        if (!orders.isEmpty()) {
            List<Bson> bsonOrders = new ArrayList<>();
            for (MongoSelectBuilder.Order order : orders) {
                if (order.asc) {
                    bsonOrders.add(Sorts.ascending(sqlService.getColumnName(order.field)));
                } else {
                    bsonOrders.add(Sorts.descending(sqlService.getColumnName(order.field)));
                }
            }
            findIterable.sort(Sorts.orderBy(bsonOrders));
        }
        if (top != -1) {
            findIterable.limit(top);
        }
        MongoCursor<Document> iterator = findIterable
              .projection(include)
              .iterator();
        return new DocumentsIterator(currentDoc, iterator, lastFullRequest);
    }

    public GlobStream execute() {
        DocumentsIterator iterator = getDocumentsIterator();
        return new GlobStream() {
            public boolean next() {
                if (iterator.hasNext()) {
                    iterator.next();
                    return true;
                }
                return false;
            }

            public Collection<Field> getFields() {
                return fieldsAndAccessor.keySet();
            }

            public Accessor getAccessor(Field field) {
                return fieldsAndAccessor.get(field);
            }

            public void close() {
                iterator.close();
            }
        };
    }

    public GlobList executeAsGlobs() {
        GlobStream globStream = execute();
        AccessorGlobsBuilder accessorGlobsBuilder = AccessorGlobsBuilder.init(globStream);
        GlobList result = new GlobList();
        while (globStream.next()) {
            result.addAll(accessorGlobsBuilder.getGlobs());
        }
        globStream.close();
        return result;
    }

    public Glob executeUnique() throws ItemNotFound, TooManyItems {
        GlobList globs = executeAsGlobs();
        if (globs.size() == 1) {
            return globs.get(0);
        }
        if (globs.isEmpty()) {
            throw new ItemNotFound("No result returned for: " + lastFullRequest);
        }
        throw new TooManyItems("Too many results for: " + lastFullRequest);
    }

    public void close() {
    }

    private static class DocumentsIterator implements Iterator<Object> {
        private Ref<Document> currentDoc;
        private MongoCursor<Document> iterator;
        private String lastFullRequest;

        public DocumentsIterator(Ref<Document> currentDoc, MongoCursor<Document> iterator, String lastFullRequest) {
            this.currentDoc = currentDoc;
            this.iterator = iterator;
            this.lastFullRequest = lastFullRequest;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public Object next() {
            Document next = iterator.next();
            currentDoc.set(next);
            return next;
        }

        public void close() {
            if (iterator.hasNext()) {
                LOGGER.warn("All data not read : for " + lastFullRequest);
            }
        }
    }

    private static class MongoConstraintVisitor implements ConstraintVisitor {

        private SqlService sqlService;
        public Bson filter;

        public MongoConstraintVisitor(SqlService sqlService) {
            this.sqlService = sqlService;
        }

        public void visitEqual(EqualConstraint constraint) {
            ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftOp);
            ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
            constraint.getRightOperand().visitOperand(rightOp);

            if (leftOp.field != null && rightOp.field == null) {
                Object value = adaptData(leftOp.field, rightOp.value);
                filter = Filters.eq(sqlService.getColumnName(leftOp.field), value);
            } else if (rightOp.field != null && leftOp.field == null) {
                Object value = adaptData(rightOp.field, leftOp.value);
                filter = Filters.eq(sqlService.getColumnName(rightOp.field), value);
            } else {
                throw new RuntimeException("Can only call equal between field and value");
            }
        }

        private Object adaptData(Field field, Object value) {
            if (field.hasAnnotation(DbRef.KEY) || (field.isKeyField() && field.getGlobType().getKeyFields().length == 1)) {
                Object value1 = value;
                if (value1 instanceof String) {
                    value = new ObjectId((String) value1);
                } else {
                    return value;
                }
            }
            return value;
        }

        public void visitNotEqual(NotEqualConstraint constraint) {
            ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftOp);
            ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
            constraint.getRightOperand().visitOperand(rightOp);

            if (leftOp.field != null && rightOp.field == null) {
                filter = Filters.ne(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
            } else if (rightOp.field != null && leftOp.field == null) {
                filter = Filters.ne(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
            } else {
                throw new RuntimeException("Can only call equal between field and value");
            }
        }

        public void visitAnd(AndConstraint constraint) {
            MongoConstraintVisitor left = new MongoConstraintVisitor(sqlService);
            constraint.getLeftConstraint().visit(left);

            MongoConstraintVisitor right = new MongoConstraintVisitor(sqlService);
            constraint.getRightConstraint().visit(right);

            filter = and(left.filter, right.filter);
        }

        public void visitOr(OrConstraint constraint) {
            MongoConstraintVisitor left = new MongoConstraintVisitor(sqlService);
            constraint.getLeftConstraint().visit(left);

            MongoConstraintVisitor right = new MongoConstraintVisitor(sqlService);
            constraint.getRightConstraint().visit(right);

            filter = or(left.filter, right.filter);
        }

        public void visitLessThan(LessThanConstraint constraint) {
            ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
            constraint.getLeftOperand().visitOperand(leftOp);
            ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
            constraint.getRightOperand().visitOperand(rightOp);

            if (leftOp.field != null && rightOp.field == null) {
                filter = Filters.lte(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
            } else if (rightOp.field != null && leftOp.field == null) {
                filter = Filters.gt(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
            } else {
                throw new RuntimeException("Can only call equal between field and value");
            }
        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {
            throw new RuntimeException("Not implemented");
        }

        public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
            throw new RuntimeException("Not implemented");
        }

        public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
            throw new RuntimeException("Not implemented");
        }

        public void visitIn(InConstraint constraint) {
            String fieldName = sqlService.getColumnName(constraint.getField());
            List<Object> converted = new ArrayList<>();
            for (Object o : constraint.getValues()) {
                converted.add(adaptData(constraint.getField(), o));
            }
            filter = Filters.in(fieldName, converted);
        }

        public void visitIsOrNotNull(NullOrNotConstraint constraint) {
            String fieldName = sqlService.getColumnName(constraint.getField());
            if (constraint.checkNull()) {
                filter = Filters.not(Filters.exists(fieldName));
            } else {
                filter = Filters.exists(fieldName);
            }
        }

        public void visitNotIn(NotInConstraint constraint) {
            String fieldName = sqlService.getColumnName(constraint.getField());
            List<Object> converted = new ArrayList<>();
            for (Object o : constraint.getValues()) {
                converted.add(adaptData(constraint.getField(), o));
            }
            filter = Filters.nin(fieldName, converted);

        }

        public void visitContains(Field field, String value, boolean contains) {
            Bson regex = Filters.regex(sqlService.getColumnName(field), ".*" + value + ".*");
            if (contains) {
                filter = regex;
            } else {
                filter = Filters.not(regex);
            }
        }

        private static class ExtractOperandVisitor implements OperandVisitor {
            private Object value;
            private Field field;

            public void visitValueOperand(ValueOperand value) {
                this.value = value.getValue();
            }

            public void visitAccessorOperand(AccessorOperand accessorOperand) {
                value = accessorOperand.getAccessor().getObjectValue();
            }

            public void visitFieldOperand(Field field) {
                this.field = field;
            }
        }
    }
}
