package com.strongduanmu.feature.traffic;

import com.google.common.collect.Sets;
import com.strongduanmu.datasource.SchemaFeatureType;
import com.strongduanmu.datasource.YamlDataSourceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Traffic test.
 */
public class TrafficTest {
    
    private Connection connection;
        
    @Before
    public void setUp() throws SQLException, IOException {
        DataSource dataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.SHARDING_DATABASES_AND_TABLES);
        connection = dataSource.getConnection();
    }
    
    @Test
    public void testSQLMatchTrafficWhenUsePreparedStatementAlgorithmMatch() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM t_order WHERE content IN (?, ?)");
        statement.setString(1, "test1");
        statement.setString(2, "test10");
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
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1");
        statement.setString(1, "test1");
        statement.setString(2, "test10");
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
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
