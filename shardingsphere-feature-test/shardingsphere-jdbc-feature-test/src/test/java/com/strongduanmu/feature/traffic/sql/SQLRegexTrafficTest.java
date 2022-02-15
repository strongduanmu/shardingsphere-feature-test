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
 * SQL regex traffic test.
 */
public class SQLRegexTrafficTest extends AbstractTrafficTest {
    
    @Test
    public void testSQLRegexTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        String sql = "SELECT * FROM t_order WHERE user_id IN (?, ?)";
        PreparedStatement statement = getPreparedStatement(sql, Arrays.asList(2, 3));
        statement.executeQuery();
        ResultSet resultSet = statement.getResultSet();
        int count = 0;
        while (resultSet.next()) {
            count++;
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                System.out.print(resultSet.getObject(index + 1) + " ");
            }
            assertTrue(Sets.newHashSet(2, 3).contains(resultSet.getInt(2)));
            System.out.println();
        }
        assertThat(count, is(3));
    }
    
    @Test
    public void testSQLRegexTrafficWhenUsePreparedStatementAlgorithmNotMatch() throws SQLException {
        String sql = "SELECT * FROM t_order WHERE 1 = 1 AND user_id IN (?, ?)";
        PreparedStatement statement = getPreparedStatement(sql, Arrays.asList(2, 3));
        statement.executeQuery();
        ResultSet resultSet = statement.getResultSet();
        int count = 0;
        while (resultSet.next()) {
            count++;
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                System.out.print(resultSet.getObject(index + 1) + " ");
            }
            assertTrue(Sets.newHashSet(2, 3).contains(resultSet.getInt(2)));
            System.out.println();
        }
        assertThat(count, is(3));
    }
    
    @Test
    public void testSQLRegexTrafficWhenUseStatementAlgorithmMatch() throws SQLException {
        String sql = "UPDATE t_order SET creation_date = NOW() + 1 WHERE user_id = 1;";
        getStatement().execute(sql);
    }
    
    @Test
    public void testSQLRegexTrafficWhenUseStatementAlgorithmNotMatch() throws SQLException {
        String sql = "UPDATE t_order SET creation_date = NOW() + 1 WHERE content = 'test1' AND user_id = 1;";
        getStatement().execute(sql);
    }
}
