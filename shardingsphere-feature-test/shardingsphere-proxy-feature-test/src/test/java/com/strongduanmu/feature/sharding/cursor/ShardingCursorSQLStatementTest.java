package com.strongduanmu.feature.sharding.cursor;

import com.strongduanmu.datasource.proxy.HikariDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Sharding cursor sql statement test.
 */
@Slf4j
public class ShardingCursorSQLStatementTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException {
        DataSource dataSource = HikariDataSourceFactory.newInstance("org.opengauss.Driver", "jdbc:opengauss://localhost:3307", "root", "root", "sharding_db");
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testCursor() throws SQLException {
        connection.setAutoCommit(false);
        executeCursor("CURSOR \"t_order_cursor\" WITHOUT HOLD FOR SELECT * FROM t_order WHERE order_id < 100 ORDER BY order_id;");
        executeTable("FETCH 2 FROM \"t_order_cursor\";", true);
        executeTable("FETCH ALL FROM \"t_order_cursor\";", true);
        executeCursor("CURSOR \"t_order_cursor_2\" WITHOUT HOLD FOR SELECT * FROM t_order WHERE order_id < 100 ORDER BY order_id;");
        executeTable("FETCH 2 FROM \"t_order_cursor_2\";", true);
        executeTable("FETCH ALL FROM \"t_order_cursor_2\";", true);
        executeTable("CLOSE \"t_order_cursor\";", false);
        executeTable("CLOSE \"t_order_cursor_2\";", false);
        connection.commit();
    }
    
    private void executeTable(final String sql, final boolean isQuery) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            if (!isQuery) {
                return;
            }
            ResultSet resultSet = preparedStatement.getResultSet();
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
    
    private void executeCursor(final String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            ResultSet schemas = connection.getMetaData().getSchemas();
            ResultSetMetaData metaData = schemas.getMetaData();
            while (schemas.next()) {
                StringBuilder builder = new StringBuilder();
                for (int index = 0; index < metaData.getColumnCount(); index++) {
                    builder.append(metaData.getColumnLabel(index + 1)).append(": ").append(schemas.getString(index + 1)).append(", ");
                }
                System.out.println(builder);
            }   
        }
    }
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
