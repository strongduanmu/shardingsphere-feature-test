package com.strongduanmu.performance.single.route;

import org.apache.shardingsphere.infra.binder.LogicSQL;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.resource.ShardingSphereResource;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.parser.config.SQLParserRuleConfiguration;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.singletable.config.SingleTableRuleConfiguration;
import org.apache.shardingsphere.singletable.route.SingleTableSQLRouter;
import org.apache.shardingsphere.singletable.rule.SingleTableRule;
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

import javax.sql.DataSource;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Single table sql router benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class SingleTableSQLRouterBenchmark {
    
    private SingleTableSQLRouter singleTableSQLRouter;
    
    private LogicSQL logicSQL;
    
    private ShardingSphereMetaData singleDataSourceMetaData;
    
    private ShardingSphereMetaData multiDataSourceMetaData;
    
    private SingleTableRule rule;
    
    private ConfigurationProperties props;
    
    @Setup(Level.Trial)
    public void setUp() {
        singleTableSQLRouter = new SingleTableSQLRouter();
        Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>(1, 1);
        DatabaseType databaseType = DatabaseTypeRegistry.getTrunkDatabaseType("MySQL");
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ColumnMetaData columnMetaData = new ColumnMetaData("single_id", Types.INTEGER, false, false, false);
        schema.put("t_single", new TableMetaData("t_single", Collections.singletonList(columnMetaData), Collections.emptyList()));
        singleDataSourceMetaData = new ShardingSphereMetaData("sharding_db", 
                new ShardingSphereResource(createSingleDataSource(), null, null, databaseType), null, schema);
        multiDataSourceMetaData = new ShardingSphereMetaData("sharding_db", 
                new ShardingSphereResource(createMultiDataSource(), null, null, databaseType), null, schema);
        metaDataMap.put("sharding_db", singleDataSourceMetaData);
        CacheOption cacheOption = new CacheOption(65535, 2000, 4);
        Optional<SQLParserRule> sqlParserRule = Optional.of(new SQLParserRule(new SQLParserRuleConfiguration(false, cacheOption, cacheOption)));
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", sqlParserRule.get());
        String sql = "SELECT * FROM t_single";
        SQLStatement sqlStatement = sqlParserEngine.parse(sql, true);
        logicSQL = new LogicSQL(SQLStatementContextFactory.newInstance(metaDataMap, sqlStatement, "sharding_db"), sql, Collections.emptyList());
        props = new ConfigurationProperties(new Properties());
        rule = new SingleTableRule(new SingleTableRuleConfiguration(), databaseType, Collections.emptyMap(), Collections.emptyList(), props);
        rule.getSingleTableDataNodes().put("t_single", Collections.singletonList(new DataNode("ds_0", "t_single")));
    }
    
    private Map<String, DataSource> createMultiDataSource() {
        Map<String, DataSource> result = new HashMap<>(2, 1);
        result.put("ds_0", null);
        result.put("ds_1", null);
        return result;
    }
    
    private Map<String, DataSource> createSingleDataSource() {
        Map<String, DataSource> result = new HashMap<>(1, 1);
        result.put("ds_0", null);
        return result;
    }
    
    @Benchmark
    public void testCreateRouteContextWithSingleDataSource() {
        singleTableSQLRouter.createRouteContext(logicSQL, singleDataSourceMetaData, rule, props);
    }
    
    @Benchmark
    public void testCreateRouteContextWithMultiDataSource() {
        singleTableSQLRouter.createRouteContext(logicSQL, multiDataSourceMetaData, rule, props);
    }
}
