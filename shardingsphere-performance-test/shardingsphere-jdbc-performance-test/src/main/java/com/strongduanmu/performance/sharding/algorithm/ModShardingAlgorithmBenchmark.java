package com.strongduanmu.performance.sharding.algorithm;

import com.google.common.collect.Range;
import org.apache.shardingsphere.sharding.algorithm.sharding.mod.ModShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.common.DataNodeInfo;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;

/**
 * Mod sharding algorithm benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class ModShardingAlgorithmBenchmark {
    
    private static final DataNodeInfo DATA_NODE_PREFIX = new DataNodeInfo("t_order_", 1);
    
    private ModShardingAlgorithm shardingAlgorithm;
    
    private Collection<String> availableTargetNames;
    
    @Setup(Level.Trial)
    public void setUp() {
        shardingAlgorithm = new ModShardingAlgorithm();
        shardingAlgorithm.getProps().setProperty("sharding-count", "16");
        shardingAlgorithm.init();
        availableTargetNames = createAvailableTargetNames();
    }
    
    @Benchmark
    public void testPreciseDoSharding() {
        shardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_PREFIX, 15));
    }
    
    @Benchmark
    public void testRangeDoSharding() {
        shardingAlgorithm.doSharding(availableTargetNames, new RangeShardingValue<>("t_order", "order_id", DATA_NODE_PREFIX, Range.closed(11L, 12L)));
    }
    
    private Collection<String> createAvailableTargetNames() {
        return new LinkedHashSet<>(Arrays.asList("t_order_8", "t_order_9", "t_order_10", "t_order_11", "t_order_12", "t_order_13", "t_order_14", "t_order_15",
                "t_order_0", "t_order_1", "t_order_2", "t_order_3", "t_order_4", "t_order_5", "t_order_6", "t_order_7"));
    }
}
