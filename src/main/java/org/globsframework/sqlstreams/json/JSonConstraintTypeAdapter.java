package org.globsframework.sqlstreams.json;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.constraints.*;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.utils.Ref;
import org.globsframework.utils.exceptions.NotSupported;

import java.io.IOException;
import java.util.*;

public class JSonConstraintTypeAdapter extends TypeAdapter<Constraint> {
    public static final String IN = "in";
    public static final String NOT_IN = "notIn";
    public static final String OR = "or";
    public static final String AND = "and";
    public static final String LEFT = "left";
    public static final String RIGHT = "right";
    public static final String EQUAL = "equal";
    public static final String CONTAINS = "contains";
    public static final String NOT_CONTAINS = "notContains";
    public static final String IS_NULL = "isNull";
    public static final String IS_NOT_NULL = "isNotNull";
    public static final String NOT_EQUAL = "notEqual";
    public static final String LESS_THAN = "lessThan";
    public static final String STRICTLY_LESS_THAN = "strictlyLessThan";
    public static final String GREATER_THAN = "greaterThan";
    public static final String STRICTLY_GREATER_THAN = "strictlyGreaterThan";
    public static final String VALUE = "value";
    public static final String VALUES = "values";
    public static final String FIELD = "field";
    public static final String TYPE = "type";
    public static final String FIELD_NAME = "name";
    private GlobTypeResolver resolver;
    private GlobType currentType;

    static public GsonBuilder createBuilder(GlobTypeResolver globTypeResolver, GlobType currentType) {
        return new GsonBuilder()
              .registerTypeHierarchyAdapter(Constraint.class, new JSonConstraintTypeAdapter(globTypeResolver, currentType));
    }

    static public GsonBuilder register(GsonBuilder gsonBuilder, GlobTypeResolver globTypeResolver, GlobType currentType) {
        return gsonBuilder.registerTypeHierarchyAdapter(Constraint.class, new JSonConstraintTypeAdapter(globTypeResolver, currentType));
    }

    public static Gson create(GlobTypeResolver globTypeResolver, GlobType currentType) {
        return createBuilder(globTypeResolver, currentType).create();
    }

    public JSonConstraintTypeAdapter(GlobTypeResolver resolver, GlobType currentType) {
        this.resolver = resolver;
        this.currentType = currentType;
    }

    public void write(JsonWriter out, Constraint constraint) throws IOException {
        if (constraint == null) {
            out.nullValue();
            return;
        }
        out.beginObject();
        constraint.visit(new JSonConstraintVisitor(out));
        out.endObject();
    }

    public Constraint read(JsonReader in) {
        JsonParser jsonParser = new JsonParser();
        JsonElement element = jsonParser.parse(in);
        Constraint constraint = null;
        JsonObject object = (JsonObject) element;
        constraint = readConstraint(object);
        return constraint;
    }

    private Constraint readConstraint(JsonObject object) {
        Constraint constraint = null;
        Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
        if (entries.size() != 1) {
            throw new RuntimeException("Only one element expected got " + entries);
        }
        Map.Entry<String, JsonElement> entry = entries.iterator().next();

        switch (entry.getKey()) {
            case EQUAL: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new EqualConstraint(leftOp.get(), rightOp.get());
            }
            case CONTAINS:{
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), true);
            }
            case NOT_CONTAINS:{
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), false);
            }
            case NOT_EQUAL: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new NotEqualConstraint(leftOp.get(), rightOp.get());
            }
            case IS_NULL: {
                Field field = readField(((JsonObject) entry.getValue()));
                return new NullOrNotConstraint(field, true);
            }
            case IS_NOT_NULL: {
                Field field = readField(((JsonObject) entry.getValue()));
                return new NullOrNotConstraint(field, false);
            }
            case AND: {
                JsonArray array = entry.getValue().getAsJsonArray();
                for (JsonElement jsonElement : array) {
                    constraint = Constraints.and(constraint, readConstraint((JsonObject) jsonElement));
                }
                return constraint;
            }
            case OR: {
                JsonArray array = entry.getValue().getAsJsonArray();
                for (JsonElement jsonElement : array) {
                    constraint = Constraints.or(constraint, readConstraint((JsonObject) jsonElement));
                }
                return constraint;
            }
            case IN: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonArray asJsonArray = in.getAsJsonArray(VALUES);
                JsonFieldValueReaderVisitor visitor = new JsonFieldValueReaderVisitor();
                Set<Object> values = new HashSet<>();
                for (JsonElement jsonElement : asJsonArray) {
                    values.add(field.safeVisit(visitor, jsonElement).value);
                }
                return Constraints.in(field, values);
            }
            case NOT_IN: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonArray asJsonArray = in.getAsJsonArray(VALUES);
                JsonFieldValueReaderVisitor visitor = new JsonFieldValueReaderVisitor();
                Set<Object> values = new HashSet<>();
                for (JsonElement jsonElement : asJsonArray) {
                    values.add(field.safeVisit(visitor, jsonElement).value);
                }
                return Constraints.notIn(field, values);
            }
            case LESS_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new LessThanConstraint(leftOp.get(), rightOp.get());
            }
            case STRICTLY_LESS_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new StrictlyLesserThanConstraint(leftOp.get(), rightOp.get());
            }
            case GREATER_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new BiggerThanConstraint(leftOp.get(), rightOp.get());
            }
            case STRICTLY_GREATER_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new StrictlyBiggerThanConstraint(leftOp.get(), rightOp.get());
            }
        }
        throw new RuntimeException(entry.getKey() + " not managed");
    }

    private void findField(JsonObject object, Ref<Operand> leftOp, Ref<Operand> rightOp) {
        JsonObject left = object.getAsJsonObject(LEFT);
        JsonObject right = object.getAsJsonObject(RIGHT);
        JsonObject opposite = null;
        Ref<Operand> oppositeRef = null;
        JsonElement fieldObj = left.get(FIELD);
        Field field;
        if (fieldObj != null) {
            field = readField(left);
            leftOp.set(new FieldOperand(field));
            opposite = right;
            oppositeRef = rightOp;
        }
        else {
            fieldObj = right.get(FIELD);
            if (fieldObj != null) {
                field = readField(right);
                rightOp.set(new FieldOperand(field));
                opposite = left;
                oppositeRef = leftOp;
            }
            else {
                throw new RuntimeException("At least one of left or right should be a field type");
            }
        }
        if (opposite.get(VALUE) != null) {
            oppositeRef.set(new ValueOperand(field, readValue(field, opposite)));
        }
        else {
            oppositeRef.set(new FieldOperand(readField(opposite)));
        }
    }

    private Object readValue(Field field, JsonObject jsonElement) {
        JsonElement value = jsonElement.get(VALUE);
        if (value == null) {
            throw new RuntimeException("A value is expected ");
        }
        return field.safeVisit(new JsonFieldValueReaderVisitor(), value).value;
    }

    private Field readField(JsonObject object) {
        GlobType currentType = this.currentType;
        JsonObject field = object.getAsJsonObject(FIELD);
        JsonElement type = field.get(TYPE);
        if (type != null && (currentType == null || !type.getAsString().equals(currentType.getName()))) {
            currentType = resolver.get(type.getAsString());
        }
        JsonElement name = field.get(FIELD_NAME);
        if (name != null) {
            return currentType.getField(name.getAsString());
        }
        throw new RuntimeException("A field is expected ");
    }

    private static class JSonConstraintVisitor implements ConstraintVisitor, OperandVisitor, FieldValueVisitor {
        private JsonWriter jsonWriter;

        public JSonConstraintVisitor(JsonWriter jsonWriter) {
            this.jsonWriter = jsonWriter;
        }

        public void visitEqual(EqualConstraint constraint) {
            try {
                jsonWriter.name(EQUAL);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void visitBinary(BinaryOperandConstraint constraint) throws IOException {
            jsonWriter.beginObject();
            jsonWriter.name(LEFT);
            jsonWriter.beginObject();
            constraint.getLeftOperand()
                  .visitOperand(this);
            jsonWriter.endObject();

            jsonWriter.name(RIGHT);
            jsonWriter.beginObject();
            constraint.getRightOperand()
                  .visitOperand(this);
            jsonWriter.endObject();

            jsonWriter.endObject();
        }

        public void visitNotEqual(NotEqualConstraint constraint) {
            try {
                jsonWriter.name(NOT_EQUAL);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void visitBinary(BinaryConstraint constraint) throws IOException {
            jsonWriter.beginArray();
            jsonWriter.beginObject();
            constraint.getLeftConstraint()
                  .visit(this);
            jsonWriter.endObject();

            jsonWriter.beginObject();
            constraint.getRightConstraint()
                  .visit(this);
            jsonWriter.endObject();

            jsonWriter.endArray();
        }

        public void visitAnd(AndConstraint constraint) {
            try {
                jsonWriter.name(AND);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitOr(OrConstraint constraint) {
            try {
                jsonWriter.name(OR);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitLessThan(LessThanConstraint constraint) {
            try {
                jsonWriter.name(LESS_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {
            try {
                jsonWriter.name(LESS_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
            try {
                jsonWriter.name(STRICTLY_GREATER_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
            try {
                jsonWriter.name(STRICTLY_LESS_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitIn(InConstraint constraint) {
            try {
                jsonWriter.name(IN);
                jsonWriter.beginObject();
                visitFieldOperand(constraint.getField());
                jsonWriter.name(VALUES)
                      .beginArray();
                for (Object o : constraint.getValues()) {
                    constraint.getField().safeVisit(this, o);
                }
                jsonWriter.endArray();
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitIsOrNotNull(NullOrNotConstraint constraint) {
            try {
                if (constraint.checkNull()) {
                    jsonWriter.name(IS_NULL);
                }
                else {
                    jsonWriter.name(IS_NOT_NULL);
                }
                visitFieldOperand(constraint.getField());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitNotIn(NotInConstraint constraint) {
            try {
                jsonWriter.name(NOT_IN);
                jsonWriter.beginObject();
                visitFieldOperand(constraint.getField());
                jsonWriter.name(VALUES)
                      .beginArray();
                for (Object o : constraint.getValues()) {
                    constraint.getField().safeVisit(this, o);
                }
                jsonWriter.endArray();
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitContains(Field field, String value, boolean contains) {
            try {
                jsonWriter.name(contains ? CONTAINS : NOT_CONTAINS);
                jsonWriter.beginObject();
                visitFieldOperand(field);
                jsonWriter.name(VALUE).value(value);
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        public void visitValueOperand(ValueOperand valueOperand) {
            try {
                jsonWriter.name(VALUE);
                valueOperand.getField().safeVisit(this, valueOperand.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitAccessorOperand(AccessorOperand accessorOperand) {
            try {
                jsonWriter.name(VALUE);
                accessorOperand.getField().safeVisit(this, accessorOperand.getAccessor().getObjectValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitFieldOperand(Field field) {
            try {
                jsonWriter.name(FIELD);
                jsonWriter.beginObject();
                jsonWriter.name(TYPE).value(field.getGlobType().getName());
                jsonWriter.name(FIELD_NAME).value(field.getName());
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitInteger(IntegerField field, Integer value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitDouble(DoubleField field, Double value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitString(StringField field, String value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitBoolean(BooleanField field, Boolean value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitLong(LongField field, Long value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitBlob(BlobField field, byte[] value) throws Exception {
            if (value != null) {
                jsonWriter.value(Base64.getEncoder().encodeToString(value));
            }
        }

        @Override
        public void visitArray(ArrayField field, List<Glob> value) {
            throw new NotSupported("JSonConstraintVisitor: visitArray") ;
        }
    }

    static class JsonFieldValueReaderVisitor implements FieldVisitorWithContext<JsonElement> {
        Object value;

        public void visitInteger(IntegerField field, JsonElement context) throws Exception {
            value = context.getAsInt();
        }

        public void visitDouble(DoubleField field, JsonElement context) throws Exception {
            value = context.getAsDouble();
        }

        public void visitString(StringField field, JsonElement context) throws Exception {
            value = context.getAsString();
        }

        public void visitBoolean(BooleanField field, JsonElement context) throws Exception {
            value = context.getAsBoolean();

        }

        public void visitLong(LongField field, JsonElement context) throws Exception {
            value = context.getAsLong();
        }

        public void visitBlob(BlobField field, JsonElement context) throws Exception {
            value = Base64.getDecoder().decode(context.getAsString());
        }

        @Override
        public void visitArray(ArrayField field, JsonElement context) throws Exception {
            throw new NotSupported("JsonFieldValueReaderVisitor: visitArray") ;
        }
    }
}
