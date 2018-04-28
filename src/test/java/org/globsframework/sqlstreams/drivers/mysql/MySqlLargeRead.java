package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.model.DummyObject;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;

public class MySqlLargeRead {


    public static void main(String[] args) {
        JdbcSqlService jdbcSqlService =new JdbcSqlService("jdbc:mysql://localhost/clientDB?zeroDateTimeBehavior=convertToNull", "owp", "123owp");

        SqlConnection db = jdbcSqlService.getDb();

        SelectQuery query = db.getQueryBuilder(DummyObject.TYPE).selectAll().getQuery();
        GlobStream execute = query.execute();
        long count = 0;
        while (execute.next()) {
            count++;
        }
        System.out.println("MySqlLargeInsert.main " + count + " element in db.");
        db.commitAndClose();
    }
}
