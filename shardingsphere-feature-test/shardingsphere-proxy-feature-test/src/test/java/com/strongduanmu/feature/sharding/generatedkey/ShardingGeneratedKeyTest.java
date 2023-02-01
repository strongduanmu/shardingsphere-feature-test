package com.strongduanmu.feature.sharding.generatedkey;

import com.strongduanmu.datasource.proxy.HikariDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

/**
 * Sharding generated key test.
 */
@Slf4j
public class ShardingGeneratedKeyTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException {
//        DataSource dataSource = HikariDataSourceFactory.newInstance("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3307", "root", "root", "sharding_db");
        DataSource dataSource = HikariDataSourceFactory.newInstance("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306", "root", "123456", "demo_ds_0");
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testGetGeneratedKey() throws SQLException {
//        String sql = "INSERT INTO t_order(user_id, content) VALUES(?, ?), (?, ?);";
        String sql = "INSERT INTO t_order_0(user_id, content) VALUES(?, ?), (?, ?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        for (int index = 0; index < 1; index++) {
            assertGeneratedKey(preparedStatement, index);
        }
    }
    
    private void assertGeneratedKey(final PreparedStatement preparedStatement, final int index) throws SQLException {
        preparedStatement.setInt(1, index);
        preparedStatement.setString(2, "TEST" + index);
        preparedStatement.setInt(3, index + 1);
        preparedStatement.setString(4, "TEST" + index + 1);
        preparedStatement.execute();
        ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        while (generatedKeys.next()) {
            log.info("columnLabel:{}", generatedKeys.getMetaData().getColumnLabel(1));
            log.info("columnName:{}", generatedKeys.getMetaData().getColumnName(1));
            log.info("generatedKey:{}", generatedKeys.getObject(1));
            assertNotNull(generatedKeys.getMetaData().getColumnLabel(1));
            assertNotNull(generatedKeys.getMetaData().getColumnName(1));
        }
    }
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
