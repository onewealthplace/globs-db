package org.globsframework.sqlstreams.drivers.mongodb;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.IsBigDecimal;
import org.globsframework.utils.exceptions.NotSupported;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class GlobCodec implements Codec<Glob> {
    private final GlobType type;
    Map<String, Field> mongoNameToField = new HashMap<>();
    Map<Field, String> fieldToMongoName = new HashMap<>();
    FieldReaderVisitor FIELD_READER_VISITOR;
    FieldWriterVisitor FIELD_WRITER_VISITOR;

    GlobCodec(GlobType type, SqlService sqlService) {
        this.type = type;
        for (Field field : type.getFields()) {
            String columnName = sqlService.getColumnName(field);
            mongoNameToField.put(columnName, field);
            fieldToMongoName.put(field, columnName);
        }

        FIELD_READER_VISITOR = new FieldReaderVisitor();
        FIELD_WRITER_VISITOR = new FieldWriterVisitor(fieldToMongoName);
    }

    public Glob decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();

        MutableGlob mutableGlob = type.instantiate();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            Field field = mongoNameToField.get(fieldName);
            if (field != null) {
                read(reader, decoderContext, mutableGlob, field);
            }
        }

        reader.readEndDocument();
        return null;
    }

    void read(BsonReader reader, DecoderContext decoderContext, MutableGlob mutableGlob, Field field) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
        } else {
            field.safeVisit(FIELD_READER_VISITOR, reader, mutableGlob);
        }
    }


    public void encode(BsonWriter bsonWriter, Glob glob, EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();
        for (Field field : type.getFields()) {
            field.safeVisit(FIELD_WRITER_VISITOR, bsonWriter, glob);
        }
        bsonWriter.writeEndDocument();
    }

    public Class<Glob> getEncoderClass() {
        return Glob.class;
    }

    static class FieldReaderVisitor implements FieldVisitorWithTwoContext<BsonReader, MutableGlob> {

        public void visitInteger(IntegerField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            mutableGlob.set(field, reader.readInt32());
        }

        public void visitDouble(DoubleField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            if (reader.getCurrentBsonType() == BsonType.DECIMAL128) {
                mutableGlob.set(field, reader.readDecimal128().bigDecimalValue().doubleValue());
            } else {
                mutableGlob.set(field, reader.readDouble());
            }
        }

        public void visitString(StringField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            mutableGlob.set(field, reader.readString());
        }

        public void visitBoolean(BooleanField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            mutableGlob.set(field, reader.readBoolean());
        }

        public void visitLong(LongField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            if (reader.getCurrentBsonType() == BsonType.INT32) {
                mutableGlob.set(field, reader.readInt32());
            } else if (reader.getCurrentBsonType() == BsonType.INT64) {
                mutableGlob.set(field, reader.readInt64());
            } else {
                throw new RuntimeException("TODO " + reader.getCurrentBsonType() + " not converted");
            }
        }

        public void visitBlob(BlobField field, BsonReader reader, MutableGlob mutableGlob) throws Exception {
            mutableGlob.set(field, reader.readBinaryData().getData());
        }

        @Override
        public void visitArray(ArrayField field, BsonReader context, MutableGlob context2) throws Exception {
            throw new NotSupported("FieldReaderVisitor: visiyArray") ;
        }
    }

    static class FieldWriterVisitor implements FieldVisitorWithTwoContext<BsonWriter, Glob> {
        private Map<Field, String> fieldStringMap;

        public FieldWriterVisitor(Map<Field, String> fieldStringMap) {
            this.fieldStringMap = fieldStringMap;
        }

        public void visitInteger(IntegerField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            Integer value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                bsonWriter.writeInt32(value);
            }
        }

        public void visitDouble(DoubleField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            Double value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                if (field.hasAnnotation(IsBigDecimal.KEY)) {
                    bsonWriter.writeDecimal128(new Decimal128(new BigDecimal(value)));
                } else {
                    bsonWriter.writeDouble(value);
                }
            }
        }

        public void visitString(StringField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            String value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                bsonWriter.writeString(value);
            }
        }

        public void visitBoolean(BooleanField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            Boolean value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                bsonWriter.writeBoolean(value);
            }
        }

        public void visitLong(LongField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            Long value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                bsonWriter.writeInt64(value);
            }
        }

        public void visitBlob(BlobField field, BsonWriter bsonWriter, Glob glob) throws Exception {
            byte[] value = glob.get(field);
            if (value != null) {
                bsonWriter.writeName(fieldStringMap.get(field));
                bsonWriter.writeBinaryData(new BsonBinary(value));
            }
        }

        @Override
        public void visitArray(ArrayField field, BsonWriter context, Glob context2) throws Exception {
            throw new NotSupported("TODO: remove") ;
        }
    }

}
