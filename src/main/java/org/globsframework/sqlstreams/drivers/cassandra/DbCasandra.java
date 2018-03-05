package org.globsframework.sqlstreams.drivers.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbFieldName;

public class DbCasandra implements SqlService{
    private Cluster cluster;
    private String keySpace;
    private Session session;

    public DbCasandra(String host, int port, String keySpace) {
        cluster = Cluster.builder().withPort(port).addContactPoint(host).build();
        session = cluster.connect();
        this.keySpace = keySpace;
    }

    public DbCasandra(String host, String keySpace) {
        cluster = Cluster.builder().addContactPoint(host).build();
        session = cluster.connect();
        this.keySpace = keySpace;
    }

    public SqlConnection getDb() {
        return new CassandraConnection(session, this);
    }

    public String getKeySpace() {
        return keySpace;
    }

    public String getTableName(GlobType globType) {
        return globType.getName();
    }

    public String getColumnName(Field field) {
        String nativeName = getNativeName(field);
        if (nativeName.startsWith("_")){
            nativeName = nativeName.substring(1, nativeName.length());
        }
        return nativeName;
    }

    private String getNativeName(Field field) {
        Glob name = field.findAnnotation(DbFieldName.KEY);
        if (name != null) {
            return name.get(DbFieldName.NAME);
        }
        return field.getName();
    }

    public void createKeyspace(String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
              new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                    .append(keySpace).append(" WITH replication = {")
                    .append("'class':'").append(replicationStrategy)
                    .append("','replication_factor':").append(replicationFactor)
                    .append("};");

        String query = sb.toString();
        session.execute(query);
    }
}
