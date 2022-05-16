package com.strongduanmu.performance.encrypt.integration;

import com.strongduanmu.datasource.jdbc.SchemaFeatureType;
import com.strongduanmu.datasource.jdbc.YamlDataSourceFactory;
import lombok.SneakyThrows;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Empty encrypt algorithm benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(100)
@Warmup(iterations = 5, time = 10)
@Measurement(iterations = 10, time = 10)
@State(Scope.Thread)
public class EmptyEncryptAlgorithmPointSelectBenchmark {
    
    private final PreparedStatement[] encryptPreparedStatements = new PreparedStatement[10];
    
    private final PreparedStatement[] rawPreparedStatements = new PreparedStatement[10];
    
    private Connection encryptConnection;
    
    private Connection rawConnection;
    
    @Setup(Level.Trial)
    @SneakyThrows
    public void setUp() {
        encryptConnection = YamlDataSourceFactory.newInstance(SchemaFeatureType.ENCRYPT_BENCHMARK).getConnection();
        for (int i = 0; i < encryptPreparedStatements.length; i++) {
            encryptPreparedStatements[i] = encryptConnection.prepareStatement(String.format("select c from sbtest%d where id = ?", i + 1));
        }
        rawConnection = DriverManager.getConnection("jdbc:mysql://127.0.01:3306/encrypt?serverTimezone=UTC&useSSL=false", "root", "123456");
        for (int i = 0; i < rawPreparedStatements.length; i++) {
            rawPreparedStatements[i] = rawConnection.prepareStatement(String.format("select c from sbtest%d where id = ?", i + 1));
        }
    }
    
    @Benchmark
    public void oltpPointSelectWithShardingSphereJDBC() throws Exception {
        for (PreparedStatement each : encryptPreparedStatements) {
            each.setInt(1, ThreadLocalRandom.current().nextInt(10));
            each.execute();
        }
    }
    
    @Benchmark
    public void oltpPointSelectWithRawJDBC() throws Exception {
        for (PreparedStatement each : rawPreparedStatements) {
            each.setInt(1, ThreadLocalRandom.current().nextInt(10));
            each.execute();
        }
    }
    
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        for (PreparedStatement each : encryptPreparedStatements) {
            each.close();
        }
        encryptConnection.close();
        for (PreparedStatement each : rawPreparedStatements) {
            each.close();
        }
        rawConnection.close();
    }
}
