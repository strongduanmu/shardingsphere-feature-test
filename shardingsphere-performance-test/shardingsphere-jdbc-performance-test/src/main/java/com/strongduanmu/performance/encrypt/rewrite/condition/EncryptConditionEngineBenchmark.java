package com.strongduanmu.performance.encrypt.rewrite.condition;

import org.apache.shardingsphere.encrypt.api.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.type.WhereAvailable;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.resource.ShardingSphereResource;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.parser.config.SQLParserRuleConfiguration;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.api.CacheOption;
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
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * Encrypt condition engine benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class EncryptConditionEngineBenchmark {
    
    private EncryptConditionEngine encryptConditionEngine;
    
    private SQLStatementContext<?> sqlStatementContext;
    
    @Setup(Level.Trial)
    public void setUp() {
        EncryptRule encryptRule = new EncryptRule(new EncryptRuleConfiguration(emptyList(), Collections.emptyMap()), Collections.emptyMap());
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ColumnMetaData columnMetaData = new ColumnMetaData("encrypt_id", Types.INTEGER, false, false, false);
        schema.put("t_encrypt", new TableMetaData("t_encrypt", Collections.singletonList(columnMetaData), emptyList()));
        encryptConditionEngine = new EncryptConditionEngine(encryptRule, schema);
        CacheOption cacheOption = new CacheOption(65535, 2000, 4);
        Optional<SQLParserRule> sqlParserRule = Optional.of(new SQLParserRule(new SQLParserRuleConfiguration(false, cacheOption, cacheOption)));
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", sqlParserRule.get());
        String sql = "SELECT * FROM t_encrypt WHERE encrypt_id = ?";
        SQLStatement sqlStatement = sqlParserEngine.parse(sql, true);
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>(1, 1);
        ShardingSphereResource resource = new ShardingSphereResource(Collections.emptyMap(), null, null, DatabaseTypeRegistry.getTrunkDatabaseType("MySQL"));
        ShardingSphereMetaData metaData = new ShardingSphereMetaData("sharding_db", resource, null, schema);
        metaDataMap.put("sharding_db", metaData);
        sqlStatementContext = SQLStatementContextFactory.newInstance(metaDataMap, Collections.emptyList(), sqlStatement, "sharding_db");
    }
    
    @Benchmark
    public void testCreateEncryptConditions() {
        Collection<WhereSegment> whereSegments = sqlStatementContext instanceof WhereAvailable ? ((WhereAvailable) sqlStatementContext).getWhereSegments() : Collections.emptyList();
        encryptConditionEngine.createEncryptConditions(whereSegments, sqlStatementContext.getTablesContext());
    }
}
