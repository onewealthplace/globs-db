package org.globsframework.sqlstreams.drivers.cassandra;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoader;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class CassandraTest {

    @Ignore
    @Test
    public void createUpdate() {
        DbCasandra dbCasandra = new DbCasandra("localhost", "owp");
        dbCasandra.createKeyspace("SimpleStrategy", 1);
        SqlConnection db = dbCasandra.getDb();

        db.createTable(DummyType.TYPE);

        db.getCreateBuilder(DummyType.TYPE).set(DummyType.ID, 1)
              .set(DummyType.FIRST_NAME, "Alfred")
              .set(DummyType.LAST_NAME, "Dupont")
              .set(DummyType.BIRTHDAY, 33900L)
              .getRequest().run();

        db.getCreateBuilder(DummyType.TYPE).set(DummyType.ID, 2)
              .set(DummyType.FIRST_NAME, "Albert")
              .set(DummyType.LAST_NAME, "Dupond")
              .set(DummyType.BIRTHDAY, 33900L)
              .getRequest().run();

        Glob alfred = db.getQueryBuilder(DummyType.TYPE, Constraints.equal(DummyType.FIRST_NAME, "Alfred"))
              .selectAll()
              .getQuery()
              .executeUnique();
        Assert.assertEquals("Dupont", alfred.get(DummyType.LAST_NAME));

        db.getUpdateBuilder(DummyType.TYPE, Constraints.equal(DummyType.ID, 1))
              .update(DummyType.BIRTHDAY, 33901L)
              .getRequest()
              .run();

        alfred = db.getQueryBuilder(DummyType.TYPE, Constraints.equal(DummyType.FIRST_NAME, "Alfred"))
              .selectAll()
              .getQuery()
              .executeUnique();
        Assert.assertEquals(33901L, alfred.get(DummyType.BIRTHDAY, 0L));
    }

    public static class DummyType {
        public static GlobType TYPE;

        @KeyField
        public static IntegerField ID;

        public static StringField FIRST_NAME;

        public static StringField LAST_NAME;

        public static LongField BIRTHDAY;

        static {
            GlobTypeLoaderFactory.create(DummyType.class, "dummy")
                  .load();
        }
    }


}
