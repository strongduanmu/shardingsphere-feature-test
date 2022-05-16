package com.strongduanmu.performance.encrypt.rewrite.generator;

import org.apache.shardingsphere.encrypt.api.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.api.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.api.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.EncryptProjectionTokenGenerator;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.algorithm.ShardingSphereAlgorithmConfiguration;
import org.apache.shardingsphere.infra.database.DefaultDatabase;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.resource.ShardingSphereResource;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;
import org.apache.shardingsphere.infra.parser.ParserConfiguration;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Encrypt predicate column token generator benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class EncryptProjectionTokenGeneratorBenchmark {
    
    private EncryptProjectionTokenGenerator tokenGenerator;
    
    private SQLStatementContext<?> sqlStatementContext;
    
    @Setup(Level.Trial)
    public void setUp() {
        EncryptRule encryptRule = createEncryptRule();
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ColumnMetaData columnMetaData = new ColumnMetaData("encrypt_id", Types.INTEGER, false, false, false);
        schema.put("t_encrypt", new TableMetaData("t_encrypt", Collections.singletonList(columnMetaData), Collections.emptyList(), Collections.emptyList()));
        SQLStatement sqlStatement = createSqlStatement();
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>(1, 1);
        DatabaseType databaseType = DatabaseTypeEngine.getTrunkDatabaseType("MySQL");
        ShardingSphereResource resource = new ShardingSphereResource(Collections.emptyMap(), null, null, databaseType);
        Map<String, ShardingSphereSchema> schemas = Collections.singletonMap(DefaultDatabase.LOGIC_NAME, schema);
        ShardingSphereMetaData metaData = new ShardingSphereMetaData("sharding_db", databaseType, resource, null, schemas);
        metaDataMap.put("sharding_db", metaData);
        sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataMap, Collections.emptyList(), sqlStatement, "sharding_db");
        tokenGenerator = new EncryptProjectionTokenGenerator();
        tokenGenerator.setEncryptRule(encryptRule);
        tokenGenerator.setSchemas(schemas);
    }
    
    private SQLStatement createSqlStatement() {
        CacheOption cacheOption = new CacheOption(65535, 2000, 4);
        ParserConfiguration parserConfiguration = new ParserConfiguration(cacheOption, cacheOption, false);
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", parserConfiguration);
        String sql = "SELECT password FROM t_encrypt WHERE encrypt_id = ?";
        return sqlParserEngine.parse(sql, true);
    }
    
    private EncryptRule createEncryptRule() {
        Collection<EncryptColumnRuleConfiguration> columns = Collections.singletonList(new EncryptColumnRuleConfiguration("password", "password", null, null, "TEST"));
        Collection<EncryptTableRuleConfiguration> tables = Collections.singletonList(new EncryptTableRuleConfiguration("t_encrypt", columns, true));
        Properties props = new Properties();
        props.setProperty("aes-key-value", "test13123");
        return new EncryptRule(new EncryptRuleConfiguration(tables, Collections.singletonMap("TEST", new ShardingSphereAlgorithmConfiguration("AES", props))), Collections.emptyMap());
    }
    
    @Benchmark
    public void testGenerateSQLTokens() {
        tokenGenerator.generateSQLTokens(sqlStatementContext);
    }
}
