package com.strongduanmu.performance.traffic.transaction.case1;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * SQL hint traffic not match compare JDBC in transaction benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(10)
@State(Scope.Thread)
public class SQLHintTrafficNotMatchCompareJDBCInTransactionBenchmark {
    
    private Connection jdbcConnection;
    
    private Connection trafficConnection;
    
    private PreparedStatement jdbcPreparedStatement;
    
    private PreparedStatement trafficPreparedStatement;
    
    @SneakyThrows
    @Setup(Level.Trial)
    public void setUp() {
        DataSource jdbcDataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.SHARDING_DATABASES_AND_TABLES);
        DataSource trafficDataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.MODE_SHARDING_DATABASES_AND_TABLES);
        jdbcConnection = jdbcDataSource.getConnection();
        trafficConnection = trafficDataSource.getConnection();
        String sql = "/* shardingsphere hint:useTraffic=false */SELECT * FROM t_order WHERE content IN (?, ?) AND user_id = 1";
        jdbcPreparedStatement = jdbcConnection.prepareStatement(sql);
        trafficPreparedStatement = trafficConnection.prepareStatement(sql);
    }
    
    @Benchmark
    public void testSQLHintTrafficNotMatchInTransaction() throws SQLException {
        trafficConnection.setAutoCommit(false);
        trafficPreparedStatement.setString(1, "test1");
        trafficPreparedStatement.setString(2, "test10");
        trafficPreparedStatement.executeQuery();
        ResultSet resultSet = trafficPreparedStatement.getResultSet();
        while (resultSet.next()) {
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                resultSet.getObject(index + 1);
            }
        }
        trafficConnection.commit();
        resultSet.close();
    }
    
    @Benchmark
    public void testShardingSphereJDBCInTransaction() throws SQLException {
        jdbcConnection.setAutoCommit(false);
        jdbcPreparedStatement.setString(1, "test1");
        jdbcPreparedStatement.setString(2, "test10");
        jdbcPreparedStatement.executeQuery();
        ResultSet resultSet = jdbcPreparedStatement.getResultSet();
        while (resultSet.next()) {
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                resultSet.getObject(index + 1);
            }
        }
        jdbcConnection.commit();
        resultSet.close();
    }
    
    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(SQLHintTrafficNotMatchCompareJDBCInTransactionBenchmark.class.getSimpleName()).forks(1).measurementIterations(5).warmupIterations(5).build();
        new Runner(options).run();
    }
    
    @SneakyThrows
    @TearDown(Level.Trial)
    public void tearDown() {
        jdbcPreparedStatement.close();
        trafficPreparedStatement.close();
        jdbcConnection.close();
        trafficConnection.close();
    }
}
