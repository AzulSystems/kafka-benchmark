# Kafka Benchmark

The Kafka e2e (end-to-end) benchmark starts multiple threads defined by number of producers and consumers.
The producers and consumers start producing and consuming messages in parallel. 
The message processing time is defined by start time when it is sent in a producer and finish time is measured in a consumer.
The benchmark measures and reports: 
* end-to-end latency percentiles (p0, p50, p99, p99.9, p99.99, p100)
* actual rate

Build:

```
# requires: java, maven 
$ ./build.sh
```

Run:

```
# requires: java 
$ java -jar kafka-benchmark-*.jar [-f yaml-file | -s yaml-string | -p prop1=value1 -p prop2=value2 ...]
```

Parameters:

```
targetRate - op/s, expected target throughput
warmupTime - sec, test warmup time
runTime - sec, test run time
intervalLength - ms, histogram writer interval
reset - call reset before each benchmark run by 'Runner'
histogramsDir - location for histogram files
brokerList - list of Kafka brokers
topic - Kafka topic used for testing
partitions - Kafka topic partitions number used in the topic creation inside benchmark reset() if BenchmarkConfig.reset is true
replicationFactor - Kafka topic replication factor used in the topic creation inside benchmark reset() if BenchmarkConfig.reset is true
createTopicRetries - number of topic creation retries
waitAfterDeleteTopic - seconds, time to do nothing after topic deletion
messageLength - minimum size of a message in bytes
messageLengthMax - maximum size of a message in bytes if > messageLength, else = messageLength
producerThreads - number of producer threads 
consumerThreads - number of consumer threads
pollTimeout - consumer poll timeout 
producerAcks - ProducerConfig.ACKS_DOC
batchSize - ProducerConfig.BATCH_SIZE_CONFIG  

```

Run examples:

```
$ java -jar kafka-benchmark-*.jar # run with default benchmark parameters

$ java -jar kafka-benchmark-*.jar -p targetRate=5000 -p warmupTime=0 -p runTime=20 -p producerThreads=4 -p consumerThreads=4 -p partitions=4

$ java -jar kafka-benchmark-*.jar -s "
targetRate: 10000
warmupTime: 30
runTime: 120
"

$ java -jar kafka-benchmark-*.jar -f kafka.config 
$ cat kafka.config
targetRate: 10000
warmupTime: 30
runTime: 120


```

Output report example:

```
2021-09-10 05:31:54,122,UTC [KafkaE2EBenchmark] Requested MR: 65000 msgs/s
2021-09-10 05:31:54,122,UTC [KafkaE2EBenchmark] Producer MR: 62057 msgs/s
2021-09-10 05:31:54,122,UTC [KafkaE2EBenchmark] Consumer MR: 62057 msgs/s
2021-09-10 05:31:54,123,UTC [KafkaE2EBenchmark] Producer 60.42 MiB/s
2021-09-10 05:31:54,123,UTC [KafkaE2EBenchmark] Consumer 60.43 MiB/s
2021-09-10 05:31:54,123,UTC [KafkaE2EBenchmark] Producer msgs count: 37366282
2021-09-10 05:31:54,123,UTC [KafkaE2EBenchmark] Consumer msgs count: 37366282
2021-09-10 05:31:54,123,UTC [KafkaE2EBenchmark] Producer total sent: 36384 MiB (38150973922)
2021-09-10 05:31:54,124,UTC [KafkaE2EBenchmark] Consumer total read: 36384 MiB (38150973922)
2021-09-10 05:31:54,124,UTC [KafkaE2EBenchmark] Producer errors: 0
2021-09-10 05:31:54,124,UTC [KafkaE2EBenchmark] Producer time: 603401 ms
2021-09-10 05:31:54,124,UTC [KafkaE2EBenchmark] Consumer time: 603353 ms
2021-09-10 05:31:54,124,UTC [KafkaE2EBenchmark] Producer msgs count during warmup: 36122933
2021-09-10 05:31:54,125,UTC [KafkaE2EBenchmark] Consumer msgs count during warmup: 36122933
2021-09-10 05:31:54,125,UTC [BasicRunner] Run finished: kafka-benchmark
2021-09-10 05:31:54,126,UTC [BasicRunner] Results:
2021-09-10 05:31:54,126,UTC [BasicRunner] Count: 37366282
2021-09-10 05:31:54,126,UTC [BasicRunner] Time: 603 s
2021-09-10 05:31:54,126,UTC [BasicRunner] Rate: 62057 msgs/s
2021-09-10 05:31:54,127,UTC [BasicRunner] Errors: 0
2021-09-10 05:31:54,127,UTC [BasicRunner] 0th percentile service time: 0.533 ms
2021-09-10 05:31:54,128,UTC [BasicRunner] 50th percentile service time: 5.272 ms
2021-09-10 05:31:54,129,UTC [BasicRunner] 90th percentile service time: 4345 ms
2021-09-10 05:31:54,129,UTC [BasicRunner] 99th percentile service time: 4731 ms
2021-09-10 05:31:54,130,UTC [BasicRunner] 99.9th percentile service time: 4840 ms
2021-09-10 05:31:54,131,UTC [BasicRunner] 99.99th percentile service time: 4861 ms
2021-09-10 05:31:54,131,UTC [BasicRunner] 100th percentile service time: 4882 ms

```




# Example test configuration on AWS

### AWS:
* AMI: ami-0747bdcabd34c712a (UBUNTU18)
* 1 node (c5.2xlarge) - for Zookeeper and kafka-e2e-benchmark 
* 3 nodes (r5d.2xlarge) - for Kafka brokers

### Setup:
mount SSD drives on r5d.2xlarge instances which are provided with them automatically:

```
$ sudo mkdir -p /localhome
$ sudo mkfs -t ext4 /dev/nvme1n1
$ sudo mount /dev/nvme1n1 /localhome
```
NOTES: drive name nvme1n1 may be different after instance start, this disk used for Kafka brokers data.

Apply recommended THP settings for new kernels:

```
$ echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
$ echo advise | sudo tee /sys/kernel/mm/transparent_hugepage/shmem_enabled
$ echo defer | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
$ echo 1 | sudo tee /sys/kernel/mm/transparent_hugepage/khugepaged/defrag
```

Kafka cluster parameters:
* Kafka broker heap 40G
* Kafka Zookeeper heap 1G

Kafka benchmark parameters:
* partitions 3 
* replicationFactor 3
* producerThreads 3
* consumerThreads 3
* acks 1
* messageLength 1000
* batchSize 0
* warmupTime 600 s
* runTime 600 s
