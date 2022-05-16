package com.strongduanmu.feature.sharding.schema;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

/**
 * Sharding schema based SQL statement test.
 */
@Slf4j
public class ShardingSchemaBasedSQLStatementTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException, IOException {
        DataSource dataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.SHARDING_DATABASES_AND_TABLES);
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testGetGeneratedKey() throws SQLException {
        String sql = "INSERT INTO t_order(user_id, content) VALUES(?, ?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "TEST");
        preparedStatement.executeUpdate();
        ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        int columnCount = generatedKeys.getMetaData().getColumnCount();
        while (generatedKeys.next()) {
            for (int index = 0; index < columnCount; index++) {
                log.info("generatedKey:{}", generatedKeys.getObject(index + 1));
                assertNotNull(generatedKeys.getMetaData().getColumnLabel(index + 1));
                assertNotNull(generatedKeys.getMetaData().getColumnName(index + 1));
            }
        }
    }
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
