package com.strongduanmu.datasource.proxy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Hikari dataSource factory.
 */
public class HikariDataSourceFactory {
    
    /**
     * New instance.
     * 
     * @param userName userName
     * @param password password
     * @param database database
     * @return DataSource
     */
    public static DataSource newInstance(final String userName, final String password, final String database) {
        HikariConfig result = new HikariConfig();
        result.setUsername(userName);
        result.setPassword(password);
        result.setDriverClassName("org.postgresql.Driver");
        result.setJdbcUrl("jdbc:postgresql://127.0.0.1:3307/" + database);
        result.setMaximumPoolSize(10);
        result.setMinimumIdle(1);
        result.setIdleTimeout(2000);
        result.setMaxLifetime(2000);
        result.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        return new HikariDataSource(result);
    }
}
