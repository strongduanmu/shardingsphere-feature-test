package com.strongduanmu.performance.single.route;

import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.context.ConnectionContext;
import org.apache.shardingsphere.infra.database.DefaultDatabase;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.ShardingSphereResourceMetaData;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
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
    
    private QueryContext logicSQL;
    
    private ShardingSphereDatabase singleDataSourceDatabase;
    
    private ShardingSphereDatabase multiDataSourceDatabase;
    
    private SingleTableRule rule;
    
    private ConfigurationProperties props;
    
    @Setup(Level.Trial)
    public void setUp() {
        singleTableSQLRouter = new SingleTableSQLRouter();
        Map<String, ShardingSphereDatabase> databaseMap = new HashMap<>(1, 1);
        DatabaseType databaseType = DatabaseTypeEngine.getTrunkDatabaseType("MySQL");
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ShardingSphereColumn columnMetaData = new ShardingSphereColumn("single_id", Types.INTEGER, false, false, false, true);
        schema.putTable("t_single", new ShardingSphereTable("t_single", Collections.singletonList(columnMetaData), Collections.emptyList(), Collections.emptyList()));
        Map<String, ShardingSphereSchema> schemas = Collections.singletonMap(DefaultDatabase.LOGIC_NAME, schema);
        singleDataSourceDatabase = new ShardingSphereDatabase("sharding_db", databaseType, new ShardingSphereResourceMetaData("sharding_db", createSingleDataSource()), null, schemas);
        multiDataSourceDatabase = new ShardingSphereDatabase("sharding_db", databaseType, new ShardingSphereResourceMetaData("sharding_db", createMultiDataSource()), null, schemas);
        databaseMap.put("sharding_db", singleDataSourceDatabase);
        CacheOption cacheOption = new CacheOption(65535, 2000);
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", cacheOption, cacheOption, false);
        String sql = "SELECT * FROM t_single";
        SQLStatement sqlStatement = sqlParserEngine.parse(sql, true);
        logicSQL = new QueryContext(SQLStatementContextFactory.newInstance(databaseMap, sqlStatement, "sharding_db"), sql, Collections.emptyList());
        props = new ConfigurationProperties(new Properties());
        rule = new SingleTableRule(new SingleTableRuleConfiguration(), DefaultDatabase.LOGIC_NAME, Collections.emptyMap(), Collections.emptyList());
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
        singleTableSQLRouter.createRouteContext(logicSQL, singleDataSourceDatabase, rule, props, new ConnectionContext());
    }
    
    @Benchmark
    public void testCreateRouteContextWithMultiDataSource() {
        singleTableSQLRouter.createRouteContext(logicSQL, multiDataSourceDatabase, rule, props, new ConnectionContext());
    }
}
