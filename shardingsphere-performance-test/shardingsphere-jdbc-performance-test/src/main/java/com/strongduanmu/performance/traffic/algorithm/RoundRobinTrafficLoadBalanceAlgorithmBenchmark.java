package com.strongduanmu.performance.traffic.algorithm;

import org.apache.shardingsphere.infra.instance.metadata.InstanceMetaData;
import org.apache.shardingsphere.infra.instance.metadata.proxy.ProxyInstanceMetaData;
import org.apache.shardingsphere.traffic.algorithm.loadbalance.RoundRobinTrafficLoadBalanceAlgorithm;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Round-robin traffic load balance algorithm benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(5)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class RoundRobinTrafficLoadBalanceAlgorithmBenchmark {
    
    private RoundRobinTrafficLoadBalanceAlgorithm loadBalanceAlgorithm;
    
    private List<InstanceMetaData> instanceMetaDataList;
    
    @Setup(Level.Trial)
    public void setUp() {
        loadBalanceAlgorithm = new RoundRobinTrafficLoadBalanceAlgorithm();
        InstanceMetaData instanceMetaData1 = new ProxyInstanceMetaData("a", "127.0.0.1@3307");
        InstanceMetaData instanceMetaData2 = new ProxyInstanceMetaData("b", "127.0.0.1@3308");
        instanceMetaDataList = Arrays.asList(instanceMetaData1, instanceMetaData2);
    }
    
    @Benchmark
    public void testGetInstanceId() {
        loadBalanceAlgorithm.getInstanceId("traffic", instanceMetaDataList);
    }
}
