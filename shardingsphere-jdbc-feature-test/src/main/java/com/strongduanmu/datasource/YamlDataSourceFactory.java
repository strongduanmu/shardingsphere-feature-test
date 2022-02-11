package com.strongduanmu.datasource;


import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Yaml dataSource factory.
 */
public final class YamlDataSourceFactory {
    
    public static DataSource newInstance(final SchemaFeatureType schemaFeatureType) throws SQLException, IOException {
        switch (schemaFeatureType) {
            case SHARDING_DATABASES:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/sharding-databases.yaml"));
            case SHARDING_TABLES:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/sharding-tables.yaml"));
            case SHARDING_DATABASES_AND_TABLES:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/sharding-databases-tables.yaml"));
            case REPLICA_QUERY:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/replica-query.yaml"));
            case READ_WRITE_SPLITTING:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/readwrite-splitting-databases.yaml"));
            case ENCRYPT:
                return YamlShardingSphereDataSourceFactory.createDataSource(getFile("/config/encrypt.yaml"));
            default:
                throw new UnsupportedOperationException(schemaFeatureType.name());
        }
    }
    
    private static File getFile(final String fileName) {
        return new File(YamlDataSourceFactory.class.getResource(fileName).getFile());
    }
}
