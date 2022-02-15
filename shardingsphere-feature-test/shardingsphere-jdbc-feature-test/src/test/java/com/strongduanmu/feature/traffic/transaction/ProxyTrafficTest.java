package com.strongduanmu.feature.traffic.transaction;

import com.strongduanmu.feature.traffic.AbstractTrafficTest;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * Proxy traffic test.
 */
public class ProxyTrafficTest extends AbstractTrafficTest {
    
    @Test
    public void testProxyTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        String sql = "SELECT * FROM t_order WHERE content IN (?, ?)";
        setAutoCommit(false);
        getPreparedStatement(sql, Arrays.asList("test1", "test10")).executeQuery();
        getPreparedStatement(sql, Arrays.asList("test1", "test10")).executeQuery();
        commit();
    }
    
    @Test
    public void testProxyTrafficWhenUseStatementAlgorithmMatch() throws SQLException {
        String sql = "/* shardingsphere hint:use_traffic=true */UPDATE t_order SET content = CONCAT(content, '999') WHERE content = 'test1' AND user_id = 1;";
        setAutoCommit(false);
        getStatement().execute(sql);
        getStatement().execute(sql);
        rollback();
    }
}
