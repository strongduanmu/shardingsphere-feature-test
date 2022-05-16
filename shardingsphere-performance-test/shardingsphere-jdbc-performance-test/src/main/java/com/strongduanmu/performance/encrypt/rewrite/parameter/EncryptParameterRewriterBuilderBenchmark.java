package com.strongduanmu.performance.encrypt.rewrite.parameter;

import org.apache.shardingsphere.encrypt.api.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.api.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.api.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewriterBuilder;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.type.WhereAvailable;
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
import org.apache.shardingsphere.parser.config.SQLParserRuleConfiguration;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.predicate.WhereSegment;
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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Encrypt parameter rewriter builder benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class EncryptParameterRewriterBuilderBenchmark {
    
    private EncryptParameterRewriterBuilder parameterRewriterBuilder;
    
    private EncryptTokenGenerateBuilder encryptTokenGenerateBuilder;
    
    @Setup(Level.Trial)
    public void setUp() {
        Collection<EncryptColumnRuleConfiguration> columns = Collections.singletonList(new EncryptColumnRuleConfiguration("password", "password", null, null, "TEST"));
        Collection<EncryptTableRuleConfiguration> tables = Collections.singletonList(new EncryptTableRuleConfiguration("t_encrypt", columns, true));
        Properties props = new Properties();
        props.setProperty("aes-key-value", "test13123");
        EncryptRule encryptRule = new EncryptRule(new EncryptRuleConfiguration(tables, Collections.singletonMap("TEST", new ShardingSphereAlgorithmConfiguration("AES", props))), Collections.emptyMap());
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ColumnMetaData columnMetaData = new ColumnMetaData("encrypt_id", Types.INTEGER, false, false, false);
        schema.put("t_encrypt", new TableMetaData("t_encrypt", Collections.singletonList(columnMetaData), Collections.emptyList(), Collections.emptyList()));
        Map<String, ShardingSphereSchema> schemas = Collections.singletonMap(DefaultDatabase.LOGIC_NAME, schema);
        EncryptConditionEngine encryptConditionEngine = new EncryptConditionEngine(encryptRule, schemas);
        CacheOption cacheOption = new CacheOption(65535, 2000, 4);
        ParserConfiguration parserConfiguration = new ParserConfiguration(cacheOption, cacheOption, false);
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", parserConfiguration);
        String sql = "SELECT password FROM t_encrypt WHERE encrypt_id = ?";
        SQLStatement sqlStatement = sqlParserEngine.parse(sql, true);
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>(1, 1);
        DatabaseType databaseType = DatabaseTypeEngine.getTrunkDatabaseType("MySQL");
        ShardingSphereResource resource = new ShardingSphereResource(Collections.emptyMap(), null, null, databaseType);
        ShardingSphereMetaData metaData = new ShardingSphereMetaData("sharding_db", databaseType, resource, null, schemas);
        metaDataMap.put("sharding_db", metaData);
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataMap, Collections.emptyList(), sqlStatement, "sharding_db");
        Collection<WhereSegment> whereSegments = sqlStatementContext instanceof WhereAvailable ? ((WhereAvailable) sqlStatementContext).getWhereSegments() : Collections.emptyList();
        Collection<ColumnSegment> columnSegments = sqlStatementContext instanceof WhereAvailable ? ((WhereAvailable) sqlStatementContext).getColumnSegments() : Collections.emptyList();
        Collection<EncryptCondition> encryptConditions = encryptConditionEngine.createEncryptConditions(whereSegments, columnSegments, sqlStatementContext, DefaultDatabase.LOGIC_NAME);
        parameterRewriterBuilder = new EncryptParameterRewriterBuilder(encryptRule, "sharding_db", schemas, sqlStatementContext, encryptConditions);
        encryptTokenGenerateBuilder = new EncryptTokenGenerateBuilder(encryptRule, sqlStatementContext, encryptConditions, "sharding_db");
    }
    
    @Benchmark
    public void testGetParameterRewriters() {
        parameterRewriterBuilder.getParameterRewriters();
    }
    
    @Benchmark
    public void testGetSQLTokenGenerators() {
        encryptTokenGenerateBuilder.getSQLTokenGenerators();
    }
}
