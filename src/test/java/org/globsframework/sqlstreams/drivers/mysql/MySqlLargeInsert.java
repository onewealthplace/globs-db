package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.model.DummyObject;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;

public class MySqlLargeInsert {


    public static void main(String[] args) {
        JdbcSqlService jdbcSqlService =new JdbcSqlService("jdbc:mysql://localhost/clientDB?zeroDateTimeBehavior=convertToNull", "owp", "123owp");

        SqlConnection db = jdbcSqlService.getDb();
        db.createTable(DummyObject.TYPE);

        SqlRequest deleteRequest = db.getDeleteRequest(DummyObject.TYPE);
        deleteRequest.run();
        deleteRequest.close();
        db.commit();

        SelectQuery query = db.getQueryBuilder(DummyObject.TYPE).selectAll().getQuery();
        GlobStream execute = query.execute();
        long count = 0;
        while (execute.next()) {
            count++;
        }
        System.out.println("MySqlLargeInsert.main " + count + " element in db.");

        ValueIntegerAccessor id = new ValueIntegerAccessor();
        SqlRequest request = db.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, id)
                .set(DummyObject.NAME, "a name")
                .getRequest();

        for (int i = 0; i < 1000000; i++) {
            id.setValue(i);
            request.run();
            if (i % 1000 == 0) {
                db.commit();
            }
        }
        request.close();
        db.commitAndClose();
    }
}
