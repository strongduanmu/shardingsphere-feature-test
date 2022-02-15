package com.strongduanmu.feature.traffic;

import com.strongduanmu.datasource.SchemaFeatureType;
import com.strongduanmu.datasource.YamlDataSourceFactory;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Abstract traffic test
 */
public abstract class AbstractTrafficTest {
    
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException, IOException {
        DataSource dataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.MODE_SHARDING_DATABASES_AND_TABLES);
        connection = dataSource.getConnection();
    }
    
    /**
     * Get prepared statement.
     * 
     * @param sql sql 
     * @param params params
     * @return prepared statement
     * @throws SQLException sql exception
     */
    protected PreparedStatement getPreparedStatement(final String sql, final List<Object> params) throws SQLException {
        PreparedStatement result = connection.prepareStatement(sql);
        int index = 0;
        for (Object each : params) {
            result.setObject(++index, each);
        }
        return result;
    }
    
    /**
     * Get statement.
     * 
     * @return statement
     * @throws SQLException sql exception
     */
    protected Statement getStatement() throws SQLException {
        return connection.createStatement();
    }
    
    /**
     * Set auto commit.
     * 
     * @param autoCommit auto commit 
     */
    protected void setAutoCommit(boolean autoCommit) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }
    
    /**
     * commit.
     */
    protected void commit() throws SQLException {
        connection.commit();
    }
    
    /**
     * rollback.
     */
    protected void rollback() throws SQLException {
        connection.rollback();
    }
    
    @After
    public void cleanUp() throws SQLException {
        connection.close();
    }
}
