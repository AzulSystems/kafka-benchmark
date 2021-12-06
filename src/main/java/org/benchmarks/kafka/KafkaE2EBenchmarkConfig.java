package org.benchmarks.kafka;

import org.benchmarks.BenchmarkConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class KafkaE2EBenchmarkConfig extends BenchmarkConfig {
    public String brokerList = "localhost:9092"; // list of Kafka brokers
    public String topic = "test";          // Kafka topic used for testing
    public String producerAcks = "1";      // ProducerConfig.ACKS_DOC
    public int partitions = 1;             // Kafka topic partitions number used in the topic creation inside benchmark reset() if BenchmarkConfig.reset is true 
    public int replicationFactor = 1;      // Kafka topic replication factor used in the topic creation inside benchmark reset() if BenchmarkConfig.reset is true
    public int createTopicRetries = 5;     // number of topic creation retries
    public int waitAfterDeleteTopic = 3;   // seconds, time to do nothing after topic deletion
    public int messageLength = 1000;       // minimum size of a message in bytes
    public int messageLengthMax = 0;       // maximum size of a message in bytes if > messageLength, else = messageLength
    public int producerThreads = 1;        // number of producer threads 
    public int consumerThreads = 1;        // number of consumer threads
    public int pollTimeout = 5000;         // consumer poll timeout 
    public int batchSize = -1;             // ProducerConfig.BATCH_SIZE_CONFIG
    public int lingerMs = -1;              // ProducerConfig.LINGER_MS_CONFIG
    public int retentionMs = -1;           // ProducerConfig.RETENTION_MS_CONFIG
    public int retentionBytes = -1;        // ProducerConfig.RETENTION_BYTES_CONFIG
}
