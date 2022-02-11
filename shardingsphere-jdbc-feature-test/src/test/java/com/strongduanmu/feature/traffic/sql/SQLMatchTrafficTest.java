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
 * SQL match traffic test.
 */
public class SQLMatchTrafficTest extends AbstractTrafficTest {
    
    @Test
    public void testSQLMatchTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        String sql = "SELECT * FROM t_order WHERE content IN (?, ?)";
        PreparedStatement statement = getPreparedStatement(sql, Arrays.asList("test1", "test10"));
        statement.executeQuery();
        ResultSet resultSet = statement.getResultSet();
        int count = 0;
        while (resultSet.next()) {
            count++;
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                System.out.print(resultSet.getObject(index + 1) + " ");
            }
            assertTrue(Sets.newHashSet("test1", "test10").contains(resultSet.getString(3)));
            System.out.println();
        }
        assertThat(count, is(2));
    }
    
    @Test
    public void testSQLMatchTrafficWhenUsePreparedStatementAlgorithmNotMatch() throws SQLException {
        String sql = "SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
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
    public void testSQLMatchTrafficWhenUseStatementAlgorithmMatch() throws SQLException {
        String sql = "UPDATE t_order SET creation_date = NOW() WHERE user_id = 1;";
        getStatement().execute(sql);
    }
    
    @Test
    public void testSQLMatchTrafficWhenUseStatementAlgorithmNotMatch() throws SQLException {
        String sql = "UPDATE t_order SET creation_date = NOW() WHERE content = '111' AND user_id = 1;";
        getStatement().execute(sql);
    }
}
