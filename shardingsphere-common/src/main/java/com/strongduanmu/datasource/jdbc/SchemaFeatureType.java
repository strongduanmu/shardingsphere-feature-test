package com.strongduanmu.datasource.jdbc;

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
    
    DUAL_WRITE,
    
    ENCRYPT,
    
    ENCRYPT_BENCHMARK,
    
    SURSEN_ENCRYPT_BENCHMARK,
    
    SHADOW,
    
    MODE_SHARDING_DATABASES_AND_TABLES
}
