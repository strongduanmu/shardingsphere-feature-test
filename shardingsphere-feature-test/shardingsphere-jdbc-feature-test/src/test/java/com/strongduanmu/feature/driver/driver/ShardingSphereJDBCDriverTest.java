package com.strongduanmu.feature.driver.driver;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * ShardingSphere JDBC driver test.
 */
public class ShardingSphereJDBCDriverTest {
    
    public static void main(String[] args) throws SQLException {
        testGetConnectionByDriver();
    }
    
    @Test
    public static void testGetConnectionByDriver() throws SQLException {
        String driverClassName = "org.apache.shardingsphere.driver.ShardingSphereDriver";
        String jdbcUrl = "jdbc:shardingsphere:classpath:config/sharding-databases-tables.yaml";
    
        // 以 HikariCP 为例 
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setJdbcUrl(jdbcUrl);
    
        String sql = "SELECT * FROM t_order o WHERE o.user_id = ? AND o.order_id = ?";
        try (Connection connection = dataSource.getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 1);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    StringBuilder builder = new StringBuilder();
                    for (int index = 0; index < metaData.getColumnCount(); index++) {
                        builder.append(metaData.getColumnLabel(index + 1)).append(": ").append(resultSet.getString(index + 1)).append(", ");
                    }
                    System.out.println(builder);
                }
            }
        }
        dataSource.close();
    }
}
