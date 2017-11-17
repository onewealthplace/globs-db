package org.globsframework.sqlstreams.drivers.mongodb;

import com.github.fakemongo.junit.FongoAsyncRule;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.model.KeyBuilder;
import org.globsframework.model.MutableGlob;
import org.globsframework.model.impl.DefaultGlob;
import org.globsframework.model.repository.DefaultGlobRepository;
import org.globsframework.sqlstreams.SqlConnection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

public class MongoSelectTest {
    @Rule
    public FongoAsyncRule fongoAsyncRule = new FongoAsyncRule();

    @Test
    public void Select() throws Exception {
        MongoDatabase database = fongoAsyncRule.getDatabase();
        MongoDbService sqlService = new MongoDbService(database);

        MongoCollection<Glob> globMongoCollection = database
              .getCollection(sqlService.getTableName(DummyObject.TYPE), Glob.class)
              .withCodecRegistry(CodecRegistries.fromProviders(new CodecProvider() {
                  public <T> Codec<T> get(Class<T> aClass, CodecRegistry codecRegistry) {
                      if (aClass == DefaultGlob.class || aClass == Glob.class) {
                          return (Codec<T>) new GlobCodec(DummyObject.TYPE, sqlService);
                      }
                      return null;
                  }
              }));

//        MongoCollection<Document> globMongoCollection = database
//              .getCollection(sqlService.getTableName(DummyObject.TYPE), Document.class);

        insert(globMongoCollection, DummyObject.TYPE.instantiate()
              .set(DummyObject.ID, 1)
              .set(DummyObject.NAME, "name 1")
              .set(DummyObject.VALUE, 3.14), sqlService);
        insert(globMongoCollection, DummyObject.TYPE.instantiate()
              .set(DummyObject.ID, 2)
              .set(DummyObject.NAME, "name 2")
              .set(DummyObject.VALUE, 3.14 * 2.), sqlService);
        insert(globMongoCollection, DummyObject.TYPE.instantiate()
              .set(DummyObject.ID, 3)
              .set(DummyObject.NAME, "name 3")
              .set(DummyObject.VALUE, 3.14 * 3.), sqlService);

        SqlConnection mangoDbConnection = new MangoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE)
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        DefaultGlobRepository globRepository = new DefaultGlobRepository();
        globRepository.add(globs);

        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 1)).get(DummyObject.NAME), "name 1");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 2)).get(DummyObject.NAME), "name 2");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 3)).get(DummyObject.VALUE), 3.14 * 3, 0.01);
    }

    private void insert(MongoCollection<Glob> globMongoCollection, MutableGlob data, MongoDbService sqlService) throws InterruptedException, java.util.concurrent.ExecutionException {
        CompletableFuture waitOn = new CompletableFuture();

        globMongoCollection.insertOne(data, (aVoid, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            }
            waitOn.complete(null);
        });
        waitOn.get();
    }

    static public class DummyObject {
        public static GlobType TYPE;

        @KeyField
        public static IntegerField ID;

        public static DoubleField VALUE;

        public static StringField NAME;

        static {
            GlobTypeLoaderFactory.create(DummyObject.class)
                  .load();
        }
    }



}
