package org.benchmarks.kafka;

import org.benchmarks.BasicRunner;
import org.benchmarks.tools.LoggerTool;

public class KafkaE2EBenchmarkRunner {
    public static void main(String[] args) {
        LoggerTool.init("benchmark");
        new BasicRunner().run(KafkaE2EBenchmark.class, args);
    }
}
