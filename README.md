# Kafka Benchmark

The Kafka e2e (end-to-end) benchmark starts multiple threads defined by number of producers and consumers.
The producers and consumers start producing and consuming messages in parallel. 
The message processing time is defined by start time when it is sent in a producer and finish time is measured in a consumer.
The benchmark measures and reports: 
* end-to-end response/service time (p0, p50, p99, p99.9, p99.99, p100, mean)
* send response/service time (p0, p50, p99, p99.9, p99.99, p100, mean)
* poll time (p0, p50, p99, p99.9, p99.99, p100, mean)
* actual rate

##### Build:

```
$ mvn package # builds target/*jar, needs prebuilt tussle-framework

   -= or =-

$ ./build.sh # builds dependency ../tussle-framework and kafka-benchmark jar
```

##### Usage:

```
$ java -jar kafka-benchmark-*.jar [benchmark-parameters] [--runner runner-class [runner-parameters]]
where benchmark-parameters and runner-parameters have following format:
[-f yaml-file | -s yaml-string | -p prop1=value1 -p prop2=value2 ...]
default runner: BasicRunner (org.tussleframework.runners.BasicRunner)
```

##### Kafka E2E Benchmark Parameters:
* **brokerList** - list of Kafka brokers (default "localhost:9092")
* **topic** - topic name used for testing (default "test")
* **topics** - the number of tested topics (default 1)
* **partitions** - the number of topic partitions for each topic
* **replicationFactor** - topic replication factor (default 1)
* **waitAfterDeleteTopic** - seconds, time to do nothing after topic deletion
* **messageLength** - minimum message size in bytes (default 1024)
* **messageLengthMax** - maximum message size: if messageLengthMax > messageLength then all message lengths are random between min and max inclusive, otherwise all message lengths are equal to messageLength (default 0)
* **producers** - number of producer threads (default 1)
* **consumers** - number of consumer threads (default 1)
* **pollTimeout** - consumer poll timeout (default 100)
* **producerAcks** - ProducerConfig.ACKS_DOC (default 1)
* **batchSize** - ProducerConfig.BATCH_SIZE_CONFIG (default 0)
* **lingerMs** - ProducerConfig.LINGER_MS_DOC (default 0)
* **idempotence** - ProducerConfig.ENABLE_IDEMPOTENCE_DOC (default false)

##### BasicRunner Parameters:
* **intervalLength** - ms, histogram writer interval
* **histogramsDir** - location for histogram files
* **targetRate** - op/s, expected target throughput, 0 - unlimited (unthrottled) target rate (default 1k)
* **warmupTime** - sec, benchmark warmup time (default 0)
* **runTime** - sec, benchmark run time (default 1m)
* **runSteps** - number of benchmark run iteration (default 1)
* **reset** - call reset before each benchmark run (default true)

##### Run examples:

```
# run with default benchmark parameters:
$ java -jar kafka-benchmark-*.jar 

# run with inline parameters:
$ java -jar kafka-benchmark-*.jar producers=4 consumers=4 partitions=4 --runner BasicRunner targetRate=5k warmupTime=0 runTime=20

# use YAML string for parameters
$ java -jar kafka-benchmark-*.jar --runner BasicRunner -s "
targetRate: 10k
warmupTime: 10s
runTime: 1m
"

# use YAML file
$ cat run.config
targetRate: 20k
warmupTime: 20s
runTime: 3m
$ java -jar kafka-benchmark-*.jar --runner BasicRunner -f run.config 

# use two YAML files for benchmark and runner
$ cat run.config
targetRate: 20k
warmupTime: 30s
runTime: 600s
$ cat kafka.config
producers: 30
consumers: 10
partitions: 10
$ java -jar kafka-benchmark-*.jar -f kafka.config --runner BasicRunner -f run.config

```

##### Output example:

```
2022-04-22 02:14:15,482,NOVT [BasicRunner] =================================================================== 
2022-04-22 02:14:15,483,NOVT [BasicRunner] Benchmark: kafka-e2e-benchmark (step 1) 
2022-04-22 02:14:15,488,NOVT [BasicRunner] Benchmark reset... 
2022-04-22 02:14:15,490,NOVT [BasicRunner] Benchmark run at target rate 10000 op/s (100%), warmup 30 s, run time 120 s... 
2022-04-22 02:14:15,492,NOVT [KafkaE2EBenchmark] Starting 1 producer and 1 consumer, targetRate 10000, warmupTime 30s, runTime 120s 
2022-04-22 02:14:15,494,NOVT [KafkaE2EBenchmark] Consumer_10000_1 started on topic 'test_1' 
2022-04-22 02:14:15,494,NOVT [KafkaE2EBenchmark] Producer_10000_1 started on topic 'test_1' 
2022-04-22 02:14:15,524,NOVT [KafkaE2EBenchmark] warmup... 
2022-04-22 02:14:20,524,NOVT [KafkaE2EBenchmark] warmup... 
2022-04-22 02:14:40,525,NOVT [KafkaE2EBenchmark] warmup... 
2022-04-22 02:14:50,503,NOVT [HdrLogWriterTask] --------------------------------------------------------------------------------------------------------------- 
2022-04-22 02:14:50,503,NOVT [HdrLogWriterTask]           name |   time |  progr |    p50ms |    p90ms |    p99ms |   p100ms |     mean |    count |    total 
2022-04-22 02:14:50,503,NOVT [HdrLogWriterTask] --------------------------------------------------------------------------------------------------------------- 
2022-04-22 02:14:50,510,NOVT [HdrLogWriterTask]      poll serv |      5 |   4.2% |     0.11 |   0.3333 |    3.746 |    13.89 |   0.2686 |    49573 |    49573 
2022-04-22 02:14:50,523,NOVT [HdrLogWriterTask]      send resp |      5 |   4.2% |   0.1199 |    1.231 |    6.296 |    13.94 |   0.5212 |    49873 |    49873 
2022-04-22 02:14:50,526,NOVT [HdrLogWriterTask] end-to-en serv |      5 |   4.2% |   0.2359 |    1.531 |    6.959 |     14.4 |   0.6718 |    49775 |    49775 
2022-04-22 02:14:50,529,NOVT [HdrLogWriterTask]      send serv |      5 |   4.2% |   0.1137 |   0.9846 |     5.71 |    13.86 |   0.4499 |    50033 |    50033 
2022-04-22 02:14:50,532,NOVT [HdrLogWriterTask] end-to-en resp |      5 |   4.2% |   0.2428 |    1.802 |    7.655 |    14.52 |   0.7458 |    49832 |    49832 
2022-04-22 02:14:55,504,NOVT [HdrLogWriterTask]      poll serv |     10 |   8.3% |  0.09907 |   0.1656 |    1.329 |    6.869 |   0.1529 |    50009 |    99582 
2022-04-22 02:14:55,506,NOVT [HdrLogWriterTask] end-to-en resp |     10 |   8.3% |   0.2016 |   0.4854 |    3.052 |    7.078 |   0.3338 |    49766 |    99598 
2022-04-22 02:14:55,508,NOVT [HdrLogWriterTask]      send resp |     10 |   8.3% |   0.1004 |   0.2908 |    2.597 |    6.484 |   0.2126 |    49956 |    99829 
2022-04-22 02:14:55,510,NOVT [HdrLogWriterTask] end-to-en serv |     10 |   8.3% |   0.1965 |   0.4605 |    2.685 |    7.074 |   0.3121 |    49854 |    99629 
2022-04-22 02:14:55,512,NOVT [HdrLogWriterTask]      send serv |     10 |   8.3% |   0.0951 |   0.2652 |    2.095 |    6.443 |   0.1876 |    49838 |    99871 
...
2022-04-22 02:16:45,529,NOVT [KafkaE2EBenchmark] Producer_10000_1 (warmup), 299993 messages, time 30 s 
2022-04-22 02:16:45,529,NOVT [KafkaE2EBenchmark] Producer_10000_1, 1200000 messages, time 120 s 
2022-04-22 02:16:45,629,NOVT [KafkaE2EBenchmark] Consumer_10000_1 (warmup), 299781 messages, time 30 s 
2022-04-22 02:16:45,629,NOVT [KafkaE2EBenchmark] Consumer_10000_1, 1199792 messages, time 120.1 s 
2022-04-22 02:16:45,630,NOVT [KafkaE2EBenchmark] Requested MR: 10000 msg/s 
2022-04-22 02:16:45,631,NOVT [KafkaE2EBenchmark] Producer (warmup) msgs rate: 10000 msg/s 
2022-04-22 02:16:45,631,NOVT [KafkaE2EBenchmark] Producer (warmup) xfer rate: 9.765 MiB/s 
2022-04-22 02:16:45,632,NOVT [KafkaE2EBenchmark] Producer (warmup) msgs count: 299993 
2022-04-22 02:16:45,632,NOVT [KafkaE2EBenchmark] Producer (warmup) xfer size: 293 MiB (307192832) 
2022-04-22 02:16:45,633,NOVT [KafkaE2EBenchmark] Producer (warmup) time: 30000 ms 
2022-04-22 02:16:45,633,NOVT [KafkaE2EBenchmark] Producer (warmup) errors: 0 
2022-04-22 02:16:45,633,NOVT [KafkaE2EBenchmark] Consumer (warmup) msgs rate: 9993 msg/s 
2022-04-22 02:16:45,633,NOVT [KafkaE2EBenchmark] Consumer (warmup) xfer rate: 9.758 MiB/s 
2022-04-22 02:16:45,633,NOVT [KafkaE2EBenchmark] Consumer (warmup) msgs count: 299781 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Consumer (warmup) xfer size: 293 MiB (306975744) 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Consumer (warmup) time: 30000 ms 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Consumer (warmup) errors: 0 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Producer msgs rate: 10000 msg/s 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Producer xfer rate: 9.766 MiB/s 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Producer msgs count: 1200000 
2022-04-22 02:16:45,634,NOVT [KafkaE2EBenchmark] Producer xfer size: 1172 MiB (1228800000) 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Producer time: 120000 ms 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Producer errors: 0 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer msgs rate: 9991 msg/s 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer xfer rate: 9.757 MiB/s 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer msgs count: 1199792 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer xfer size: 1172 MiB (1228587008) 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer time: 120084 ms 
2022-04-22 02:16:45,635,NOVT [KafkaE2EBenchmark] Consumer errors: 0 
2022-04-22 02:16:45,636,NOVT [BasicRunner] Reguested rate 10000 msg/s (100%), actual rate 10000 msg/s 
2022-04-22 02:16:45,636,NOVT [BasicRunner] ----------------------------------------------------- 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Run finished: kafka-e2e-benchmark 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Results (step 1) 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Count: 1199792 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Time: 120 s 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Rate: 10000 msg/s 
2022-04-22 02:16:45,637,NOVT [BasicRunner] Errors: 0 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p0: 0.1101 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p50: 0.2042 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p90: 0.6052 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p99: 3.715 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p99.9: 15.79 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p99.99: 31.47 ms 
2022-04-22 02:16:45,638,NOVT [BasicRunner] end-to-end response_time p100: 36.24 ms 
2022-04-22 02:16:45,639,NOVT [BasicRunner] end-to-end response_time mean: 0.4012 ms 
2022-04-22 02:16:45,639,NOVT [BasicRunner] end-to-end service_time p0: 0.1055 ms 
2022-04-22 02:16:45,639,NOVT [BasicRunner] end-to-end service_time p50: 0.1989 ms 
2022-04-22 02:16:45,639,NOVT [BasicRunner] end-to-end service_time p90: 0.5571 ms 
2022-04-22 02:16:45,640,NOVT [BasicRunner] end-to-end service_time p99: 3.441 ms 
2022-04-22 02:16:45,641,NOVT [BasicRunner] end-to-end service_time p99.9: 15.75 ms 
2022-04-22 02:16:45,641,NOVT [BasicRunner] end-to-end service_time p99.99: 31.46 ms 
2022-04-22 02:16:45,641,NOVT [BasicRunner] end-to-end service_time p100: 36.21 ms 
2022-04-22 02:16:45,642,NOVT [BasicRunner] end-to-end service_time mean: 0.3799 ms 
2022-04-22 02:16:45,643,NOVT [BasicRunner] poll service_time p0: 0.03878 ms 
2022-04-22 02:16:45,643,NOVT [BasicRunner] poll service_time p50: 0.09965 ms 
2022-04-22 02:16:45,643,NOVT [BasicRunner] poll service_time p90: 0.1743 ms 
2022-04-22 02:16:45,643,NOVT [BasicRunner] poll service_time p99: 1.443 ms 
2022-04-22 02:16:45,644,NOVT [BasicRunner] poll service_time p99.9: 5.681 ms 
2022-04-22 02:16:45,644,NOVT [BasicRunner] poll service_time p99.99: 9.454 ms 
2022-04-22 02:16:45,644,NOVT [BasicRunner] poll service_time p100: 27.12 ms 
2022-04-22 02:16:45,644,NOVT [BasicRunner] poll service_time mean: 0.1584 ms 
2022-04-22 02:16:45,644,NOVT [BasicRunner] send response_time p0: 0.06579 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p50: 0.1009 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p90: 0.3794 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p99: 3.146 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p99.9: 15.34 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p99.99: 30.82 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time p100: 36.01 ms 
2022-04-22 02:16:45,645,NOVT [BasicRunner] send response_time mean: 0.2692 ms 
2022-04-22 02:16:45,646,NOVT [BasicRunner] send service_time p0: 0.06208 ms 
2022-04-22 02:16:45,646,NOVT [BasicRunner] send service_time p50: 0.09568 ms 
2022-04-22 02:16:45,646,NOVT [BasicRunner] send service_time p90: 0.3451 ms 
2022-04-22 02:16:45,646,NOVT [BasicRunner] send service_time p99: 2.828 ms 
2022-04-22 02:16:45,647,NOVT [BasicRunner] send service_time p99.9: 15.33 ms 
2022-04-22 02:16:45,647,NOVT [BasicRunner] send service_time p99.99: 30.82 ms 
2022-04-22 02:16:45,647,NOVT [BasicRunner] send service_time p100: 36.01 ms 
2022-04-22 02:16:45,648,NOVT [BasicRunner] send service_time mean: 0.2479 ms 
```

# Example test configuration on AWS

##### Instance setup:
* AWS image: UBUNTU18 e.g. ami-0747bdcabd34c712a (may be different depending on AWS zone and region) 
* 1 node (t2.small): Zookeeper
* 1 node (m5n.8xlarge): load generator (Kafka E2E benchmark)
* 3 nodes (i3en.2xlarge): Kafka brokers

mount SSD drives on instances for Kafka brokers which are provided with them automatically:

```
$ sudo mkdir -p /localhome
$ sudo mkfs -t ext4 /dev/nvme1n1
$ sudo mount /dev/nvme1n1 /localhome
```
Drive name nvme1n1 may be different after instance start, this disk used for Kafka brokers data.

Apply recommended THP settings for new kernels:

```
$ echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
$ echo advise | sudo tee /sys/kernel/mm/transparent_hugepage/shmem_enabled
$ echo defer | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
$ echo 1 | sudo tee /sys/kernel/mm/transparent_hugepage/khugepaged/defrag
```

##### Kafka setup:
* Kafka broker heap 40G
* Kafka Zookeeper heap 1G

##### Benchmark run:
* topics 1 (benchmark's default)
* partitions 3
* replicationFactor 3
* producers 15
* consumers 3
* acks 1 (benchmark's default)
* messageLength 1024
* batchSize 0 (benchmark's default)
* lingerMs 0 (benchmark's default)
* targetRate 0 (unthrottled)
* warmupTime 60 sec
* runTime 600 sec

Benchmark's command line:

```
$ java -jar kafka-benchmark-*.jar consumers=3 partitions=3 producers=15 rf=3 \
--runner BasicRunner targetRate=0 warmupTime=1m runTime=10m

```


