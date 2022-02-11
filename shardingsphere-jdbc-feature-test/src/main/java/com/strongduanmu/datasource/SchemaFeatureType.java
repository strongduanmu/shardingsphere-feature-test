package com.strongduanmu.datasource;

/**
 * Schema feature type.
 */
public enum SchemaFeatureType {
    
    SHARDING_DATABASES,
    
    SHARDING_TABLES,
    
    SHARDING_DATABASES_AND_TABLES,
    
    SHARDING_SHADOW_DATABASES,
    
    ENCRYPT_SHADOW,
    
    REPLICA_QUERY,
    
    REPLICA_QUERY_SHADOW,
    
    READ_WRITE_SPLITTING,
    
    ENCRYPT,
    
    SHADOW,
    
    MODE_SHARDING_DATABASES_AND_TABLES
}
