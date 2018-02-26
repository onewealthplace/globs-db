package org.globsframework.sqlstreams.drivers.mongodb;

import com.github.fakemongo.junit.FongoAsyncRule;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.Block;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoader;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.index.MultiFieldUniqueIndex;
import org.globsframework.model.GlobList;
import org.globsframework.model.KeyBuilder;
import org.globsframework.model.repository.DefaultGlobRepository;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.utils.Ref;
import org.globsframework.utils.Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.globsframework.sqlstreams.drivers.mongodb.MongoSelectTest.DummyObject.*;

public class MongoSelectTest {
    @Rule
    public FongoRule fongoRule = new FongoRule();
    @Rule
    public FongoAsyncRule fongoAsyncRule = new FongoAsyncRule();

    @Test
    public void Select() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE)
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        DefaultGlobRepository globRepository = new DefaultGlobRepository();
        globRepository.add(globs);

        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 1)).get(DummyObject.NAME), "name 1");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 2)).get(DummyObject.NAME), "name 2");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 3)).get(VALUE), 3.14 * 3, 0.01);

    }

    @Test
    public void IsNullIsExist() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.isNull(DummyObject.NAME_2))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(3, globs.size());

        globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.isNotNull(DummyObject.NAME_2))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(1, globs.size());
    }


    @Test
    public void contains() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.contains(DummyObject.NAME, "2"))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(1, globs.size());

        globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.notContains(DummyObject.NAME, "2"))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(3, globs.size());
    }

    @Test
    public void notIn() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.notIn(DummyObject.NAME, Utils.set("name 1", "name 2")))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(2, globs.size());
        Assert.assertTrue(globs.stream()
              .map(g -> g.get(DummyObject.NAME))
              .anyMatch(s -> s.equals("name 3")));
    }

    @Test
    public void orderAndLimit() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();
        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);

        GlobList sortedFirstGlob = mangoDbConnection.getQueryBuilder(DummyObject.TYPE)
              .orderDesc(VALUE)
              .orderAsc(NAME)
              .top(1)
              .selectAll()
              .getQuery().executeAsGlobs();

        Assert.assertEquals(1, sortedFirstGlob.size());
        Assert.assertEquals(4, sortedFirstGlob.get(0).get(ID).intValue());
    }

    @Test
    public void testInOp() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();
        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);

        GlobList sortedFirstGlob = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.in(DummyObject.NAME, Utils.set("name 1", "name 3")))
              .selectAll()
              .orderAsc(DummyObject.ID)
              .getQuery().executeAsGlobs();

        Assert.assertEquals(2, sortedFirstGlob.size());
        Assert.assertEquals(1, sortedFirstGlob.get(0).get(ID).intValue());
        Assert.assertEquals(3, sortedFirstGlob.get(1).get(ID).intValue());
    }

    @Test
    public void checkIndexCreation() {
        MongoDatabase database = fongoRule.getDatabase();
        com.mongodb.client.MongoCollection<Document> globMongoCollection = database.getCollection(DummyObject.TYPE.getName(), Document.class);
        MongoUtils.createIndexIfNeeded(globMongoCollection, Collections.singleton(NAME_INDEX));
        Ref<Boolean> future = new Ref<>();
        globMongoCollection.listIndexes().forEach((Block<? super Document>) document -> {
                if (MongoUtils.contain(NAME_INDEX, document)) {
                    future.set(Boolean.TRUE);
            }
        });
        Assert.assertTrue(future.get());
    }

    static public class DummyObject {
        public static GlobType TYPE;

        @KeyField
        public static IntegerField ID;

        public static DoubleField VALUE;

        public static StringField NAME;

        public static StringField NAME_2;

        public static MultiFieldUniqueIndex NAME_INDEX;

        static {
            GlobTypeLoader globTypeLoader = GlobTypeLoaderFactory.create(DummyObject.class);
            globTypeLoader.load();
            globTypeLoader.defineMultiFieldUniqueIndex(NAME_INDEX, NAME, NAME_2);
        }
    }


    private class InitDb {
        private com.mongodb.client.MongoDatabase database;
        private MongoDbService sqlService;

        public com.mongodb.client.MongoDatabase getDatabase() {
            return database;
        }

        public MongoDbService getSqlService() {
            return sqlService;
        }

        public InitDb invoke() {
            database = fongoRule.getDatabase();
            sqlService = new MongoDbService(database);
            sqlService.getDb().populate(new GlobList(
                  DummyObject.TYPE.instantiate()
                        .set(DummyObject.ID, 1)
                        .set(DummyObject.NAME, "name 1")
                        .set(DummyObject.NAME_2, "second name")
                        .set(VALUE, 3.14),
                  DummyObject.TYPE.instantiate()
                        .set(DummyObject.ID, 2)
                        .set(DummyObject.NAME, "name 2")
                        .set(VALUE, 3.14 * 2.),
                  DummyObject.TYPE.instantiate()
                        .set(DummyObject.ID, 3)
                        .set(DummyObject.NAME, "name 3")
                        .set(VALUE, 3.14 * 3.),
                  DummyObject.TYPE.instantiate()
                        .set(DummyObject.ID, 4)
                        .set(DummyObject.NAME, "my name")
                        .set(VALUE, 3.14 * 3.)));
            return this;
        }
    }
}
