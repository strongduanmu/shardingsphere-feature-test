package com.strongduanmu.feature.traffic.sql;

import com.google.common.collect.Sets;
import com.strongduanmu.feature.traffic.AbstractTrafficTest;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * SQL hint traffic test.
 */
public class SQLHintTrafficTest extends AbstractTrafficTest {
    
    @Test
    public void testSQLHintTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        String sql = "/* shardingsphere hint:useTraffic=true */SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
        PreparedStatement statement = getPreparedStatement(sql, Arrays.asList("test1", "test10"));
        statement.executeQuery();
        ResultSet resultSet = statement.getResultSet();
        int count = 0;
        while (resultSet.next()) {
            count++;
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                System.out.print(resultSet.getObject(index + 1) + " ");
            }
            assertTrue(Sets.newHashSet("test1").contains(resultSet.getString(3)));
            System.out.println();
        }
        assertThat(count, is(1));
    }
    
    @Test
    public void testSQLHintTrafficWhenUsePreparedStatementAlgorithmNotMatch() throws SQLException {
        String sql = "/* shardingsphere hint:show_traffic=true */SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
        PreparedStatement statement = getPreparedStatement(sql, Arrays.asList("test1", "test10"));
        statement.executeQuery();
        ResultSet resultSet = statement.getResultSet();
        int count = 0;
        while (resultSet.next()) {
            count++;
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                System.out.print(resultSet.getObject(index + 1) + " ");
            }
            assertTrue(Sets.newHashSet("test1").contains(resultSet.getString(3)));
            System.out.println();
        }
        assertThat(count, is(1));
    }
    
    @Test
    public void testSQLHintTrafficWhenUseStatementAlgorithmMatch() throws SQLException {
        String sql = "/* shardingsphere hint:useTraffic=true */UPDATE t_order SET content = CONCAT(content, '999') WHERE content = '111' AND user_id = 1;";
        getStatement().execute(sql);
    }
    
    @Test
    public void testSQLHintTrafficWhenUseStatementAlgorithmNotMatch() throws SQLException {
        String sql = "/* shardingsphere hint:show_traffic=true */UPDATE t_order SET content = CONCAT(content, '999') WHERE content = '111' AND user_id = 1;";
        getStatement().execute(sql);
    }
}
