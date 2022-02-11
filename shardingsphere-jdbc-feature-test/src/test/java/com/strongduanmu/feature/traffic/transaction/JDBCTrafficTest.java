package com.strongduanmu.feature.traffic.transaction;

import com.strongduanmu.feature.traffic.AbstractTrafficTest;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * JDBC traffic test.
 */
public class JDBCTrafficTest extends AbstractTrafficTest {
    
    @Test
    public void testJDBCTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        String sql1 = "/* shardingsphere hint:use_traffic=true */SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
        String sql2 = "SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
        setAutoCommit(false);
        getPreparedStatement(sql1, Arrays.asList("test1", "test10")).executeQuery();
        getPreparedStatement(sql2, Arrays.asList("test1", "test10")).executeQuery();
        commit();
    }
    
    @Test
    public void testJDBCTrafficWhenUseStatementAlgorithmMatch() throws SQLException {
        String sql1 = "/* shardingsphere hint:use_traffic=true */SELECT * FROM t_order WHERE content IN ('test1', 'test10') AND user_id = 1";
        String sql2 = "SELECT * FROM t_order WHERE content IN ('test1', 'test10') AND user_id = 1";
        setAutoCommit(false);
        getStatement().execute(sql1);
        getStatement().execute(sql2);
        rollback();
    }
}
