package com.strongduanmu.performance.traffic.sql.case2;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import com.strongduanmu.datasource.proxy.HikariDataSourceFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
 * SQL regex traffic match compare Proxy benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(10)
@State(Scope.Thread)
@Slf4j
public class SQLRegexTrafficMatchCompareProxyBenchmark {
    
    private Connection proxyConnection;
    
    private Connection trafficConnection;
    
    private PreparedStatement proxyPreparedStatement;
    
    private PreparedStatement trafficPreparedStatement;
    
    @SneakyThrows
    @Setup(Level.Trial)
    public void setUp() {
        DataSource proxyDataSource = HikariDataSourceFactory.newInstance("org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:3307", "root", "root", "sharding_db");
        DataSource trafficDataSource = YamlDataSourceFactory.newInstance(SchemaFeatureType.MODE_SHARDING_DATABASES_AND_TABLES);
        proxyConnection = proxyDataSource.getConnection();
        trafficConnection = trafficDataSource.getConnection();
        String sql = "SELECT * FROM t_order WHERE user_id IN (?, ?)";
        proxyPreparedStatement = proxyConnection.prepareStatement(sql);
        trafficPreparedStatement = trafficConnection.prepareStatement(sql);
    }
    
    @Benchmark
    public void testSQLRegexTrafficMatch() throws SQLException {
        trafficPreparedStatement.setInt(1, 2);
        trafficPreparedStatement.setInt(2, 3);
        trafficPreparedStatement.executeQuery();
        ResultSet resultSet = trafficPreparedStatement.getResultSet();
        while (resultSet.next()) {
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                resultSet.getObject(index + 1);
            }
        }
        resultSet.close();
    }
    
    /*@Benchmark
    public void testShardingSphereProxy() throws SQLException {
        proxyPreparedStatement.setInt(1, 2);
        proxyPreparedStatement.setInt(2, 3);
        proxyPreparedStatement.executeQuery();
        ResultSet resultSet = proxyPreparedStatement.getResultSet();
        while (resultSet.next()) {
            for (int index = 0; index < resultSet.getMetaData().getColumnCount(); index++) {
                log.info("column {}: {}", index + 1, resultSet.getObject(index + 1));
            }
        }
        resultSet.close();
    }*/
    
    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder().include(SQLRegexTrafficMatchCompareProxyBenchmark.class.getSimpleName()).forks(1).shouldFailOnError(true).measurementIterations(5).warmupIterations(5).build();
        new Runner(options).run();
    }
    
    @SneakyThrows
    @TearDown(Level.Trial)
    public void tearDown() {
        proxyPreparedStatement.close();
        trafficPreparedStatement.close();
        proxyConnection.close();
        trafficConnection.close();
    }
}
