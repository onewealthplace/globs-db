package org.globsframework.sqlstreams.json;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;

import java.io.IOException;
import java.util.*;

public class JSonConstraintTypeAdapter extends TypeAdapter<Constraint> {
    public static final String IN = "in";
    public static final String OR = "or";
    public static final String AND = "and";
    public static final String LEFT = "left";
    public static final String RIGHT = "right";
    public static final String EQUAL = "equal";
    public static final String NOT_EQUAL = "notEqual";
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
                Field field = readField(((JsonObject) entry.getValue()).getAsJsonObject(LEFT));
                Object value = readValue(field, ((JsonObject) entry.getValue()).getAsJsonObject(RIGHT));
                return new EqualConstraint(new FieldOperand(field), new ValueOperand(field, value));
            }
            case NOT_EQUAL: {
                Field field = readField(((JsonObject) entry.getValue()).getAsJsonObject(LEFT));
                Object value = readValue(field, ((JsonObject) entry.getValue()).getAsJsonObject(RIGHT));
                return new NotEqualConstraint(new FieldOperand(field), new ValueOperand(field, value));
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
            case IN:{
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonArray asJsonArray = in.getAsJsonArray(VALUES);
                JsonFieldValueReaderVisitor visitor = new JsonFieldValueReaderVisitor();
                List<Object> values = new ArrayList<>();
                for (JsonElement jsonElement : asJsonArray) {
                    values.add(field.safeVisit(visitor, jsonElement).value);
                }
                return Constraints.in(field, values);
            }
        }
        throw new RuntimeException(entry.getKey() + " not managed");
    }

    private Object readValue(Field field, JsonObject jsonElement) {
        JsonElement value = jsonElement.get(VALUE);
        if (value == null) {
            return null;
        }
        return field.safeVisit(new JsonFieldValueReaderVisitor(), value).value;
    }

    private Field readField(JsonObject object) {
        GlobType currentType = this.currentType;
        JsonObject field = object.getAsJsonObject(FIELD);
        JsonElement type = field.get(TYPE);
        if (type != null) {
            currentType = resolver.get(type.getAsString());
        }
        JsonElement name = field.get(FIELD_NAME);
        if (name != null) {
            return currentType.getField(name.getAsString());
        }
        return null;
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

        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {

        }

        public void visitStricklyBiggerThan(StrictlyBiggerThanConstraint constraint) {

        }

        public void visitStricklyLesserThan(StrictlyLesserThanConstraint constraint) {

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
    }
}
