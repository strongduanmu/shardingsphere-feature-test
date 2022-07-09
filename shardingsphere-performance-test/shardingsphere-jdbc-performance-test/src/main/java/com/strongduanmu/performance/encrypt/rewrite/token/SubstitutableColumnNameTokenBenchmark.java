package com.strongduanmu.performance.encrypt.rewrite.token;

import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.DefaultDatabase;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.ShardingSphereResource;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.decorator.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.parser.ShardingSphereSQLParserEngine;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.sql.common.constant.QuoteCharacter;
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
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * Substitutable column name token benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class SubstitutableColumnNameTokenBenchmark {
    
    private SubstitutableColumnNameToken token;
    
    private RouteUnit routeUnit;
    
    @Setup(Level.Trial)
    public void setUp() {
        ShardingSphereSchema schema = new ShardingSphereSchema();
        ShardingSphereColumn columnMetaData = new ShardingSphereColumn("encrypt_id", Types.INTEGER, false, false, false);
        schema.put("t_encrypt", new ShardingSphereTable("t_encrypt", Collections.singletonList(columnMetaData), emptyList(), Collections.emptyList()));
        SQLStatement sqlStatement = createSqlStatement();
        Map<String, ShardingSphereDatabase> metaDataMap = new HashMap<>(1, 1);
        DatabaseType databaseType = DatabaseTypeEngine.getTrunkDatabaseType("MySQL");
        ShardingSphereResource resource = new ShardingSphereResource(Collections.emptyMap());
        Map<String, ShardingSphereSchema> schemas = Collections.singletonMap(DefaultDatabase.LOGIC_NAME, schema);
        ShardingSphereDatabase metaData = new ShardingSphereDatabase("sharding_db", databaseType, resource, null, schemas);
        metaDataMap.put("sharding_db", metaData);
        SelectStatementContext selectStatementContext = (SelectStatementContext) SQLStatementContextFactory.newInstance(metaDataMap, Collections.emptyList(), sqlStatement, "sharding_db");
        Collection<ColumnProjection> projections = getColumnProjections(selectStatementContext);
        token = new SubstitutableColumnNameToken(0, 0, projections, QuoteCharacter.NONE);
        routeUnit = new RouteUnit(new RouteMapper("sharding_db", "ds_0"), Collections.emptyList());
    }
    
    private Collection<ColumnProjection> getColumnProjections(final SelectStatementContext selectStatementContext) {
        Collection<ColumnProjection> result = new LinkedList<>();
        for (Projection each : selectStatementContext.getProjectionsContext().getExpandProjections()) {
            if (each instanceof ColumnProjection) {
                result.add((ColumnProjection) each);
            }
        }
        return result;
    }
    
    private SQLStatement createSqlStatement() {
        CacheOption cacheOption = new CacheOption(65535, 2000);
        ShardingSphereSQLParserEngine sqlParserEngine = new ShardingSphereSQLParserEngine("MySQL", cacheOption, cacheOption, false);
        String sql = "SELECT password FROM t_encrypt WHERE encrypt_id = ?";
        return sqlParserEngine.parse(sql, true);
    }
    
    @Benchmark
    public void testGenerateSQLTokens() {
        token.toString(routeUnit);
    }
}
