package org.benchmarks.kafka;

import static org.benchmarks.tools.FormatTool.roundFormat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.benchmarks.Benchmark;
import org.benchmarks.BenchmarkConfig;
import org.benchmarks.RunResult;
import org.benchmarks.TimeRecorder;
import org.benchmarks.tools.ConfigLoader;
import org.benchmarks.tools.SleepTool;

public class KafkaE2EBenchmark implements Benchmark {

    private static final Logger logger = Logger.getLogger(KafkaE2EBenchmark.class.getName());
    private static final String RATE_MSGS_UNITS = "msgs/s";
    private static final String RATE_MB_UNITS = "MiB/s";
    static final long NS_IN_MS = 1000000L;

    private long totalConsumerMsgCountWarmup;
    private long totalConsumerMsgCount;
    private long totalProducerMsgCountW;
    private long totalProducerMsgCount;
    private long totalConsumerBytes;
    private long totalProducerBytes;
    private long totalProducerErrors;
    private double totalConsumerThroughput;
    private double totalProducerThroughput;
    private double totalConsumerBytesThroughput;
    private double totalProducerBytesThroughput;
    private long consumerTime;
    private long producerTime;
    private boolean resetRequired = true;
    private Object consumerLock = new Object();
    private Object producerLock = new Object();
    private KafkaE2EBenchmarkConfig config;
    private AdminClient adminClient;
    private Properties consumerProps;
    private Properties producerProps;

    public static void log(String format, Object... args) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info(String.format("[KafkaE2EBenchmark] %s", String.format(format, args)));
        }
    }

    public static String withS(long count, String name) {
        if (count == 1 || count == -1) {
            return count + " " + name;
        } else {
            return count + " " + name + "s";
        }
    }

    public KafkaE2EBenchmark() {
    }

    public KafkaE2EBenchmark(KafkaE2EBenchmarkConfig config) {
        this.config = config;
        initProps();
    }

    @Override
    public void init(String[] args) throws Exception {
        config = ConfigLoader.load(args, true, KafkaE2EBenchmarkConfig.class);
        initProps();
    }

    @Override
    public void cleanup() {
        deleteTopic();
    }

    @Override
    public String getName() {
        return "kafka-e2e-benchmark";
    }

    @Override
    public void reset() {
        if (resetRequired) {
            deleteTopic();
            createTopic();
            resetRequired = false;
        }
    }

    protected void initProps() {
        consumerProps = new Properties();
        producerProps = new Properties();
        setupConsumerProps(consumerProps);
        setupProducerProps(producerProps);
        Properties adminClientProps = new Properties();
        setupAdminClientProps(adminClientProps);
        adminClient = AdminClient.create(adminClientProps);
        reset();
    }

    static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(int s, String msg) {
        if (msg != null) {
            log("Sleeping " + s + " seconds - " + msg);
        }
        try {
            Thread.sleep(s * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void deleteTopic() {
        log("Deleting topic '" + config.topic + "'...");
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(config.topic));
            result.all().get();
            if (config.waitAfterDeleteTopic > 0) {
                sleep(config.waitAfterDeleteTopic, "waiting after topic deletion");
            }
            log("Topic deleted '" + config.topic + "'");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (e instanceof UnknownTopicOrPartitionException) {
                log("Cannot delete topic '" + config.topic + "' " + e);
                return;
            }
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                log("Cannot delete topic '" + config.topic + "' " + e.getCause());
                return;
            }
            throw new KafkaRuntimeException("Failed to delete topic '" + config.topic + "'", e);
        }
    }

    public void createTopic() {
        int waitTime = 1;
        int attemptNum = 0;
        int retries = config.createTopicRetries;
        Map<String, String> configs = new HashMap<>();
        if (config.retentionMs > 0) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.retentionMs));
        }
        if (config.retentionBytes > 0) {
            configs.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(config.retentionBytes));
        }
        NewTopic newTopic = new NewTopic(config.topic, config.partitions, (short) config.replicationFactor);
        if (configs.size() > 0) {
            newTopic.configs(configs);
        }
        Collection<NewTopic> newTopics = Arrays.asList(newTopic);
        while (true) {
            attemptNum++;
            try {
                log("Creating new topic '%s' with %s, replication-factor %d (attempt #%d)", config.topic, withS(config.partitions, "partition"), config.replicationFactor, attemptNum);
                CreateTopicsResult result = adminClient.createTopics(newTopics);
                result.all().get();
                log("Topic created: '%s' (attempt #%d)", config.topic, attemptNum);
                return;
            } catch (final Exception e) {
                if (retries <= 0) {
                    throw new KafkaRuntimeException("Failed to create topic '" + config.topic + "'", e);
                }
                retries--;
                log("Failed to create topic '" + config.topic + "', cause: " + e);
                sleep(waitTime, "will retry to create topic");
                waitTime++;
            }
        }
    }

    @Override
    public RunResult run(double targetRate, int warmupTime, int runTime, TimeRecorder timeRecorder) {
        double perProducerMessageRate = targetRate / config.producerThreads;
        resetRequired = true;
        consumerTime = 0;
        producerTime = 0;
        totalProducerMsgCount = 0;
        totalConsumerMsgCount = 0;
        totalProducerMsgCountW = 0;
        totalConsumerMsgCountWarmup = 0;
        totalConsumerBytes = 0;
        totalProducerBytes = 0;
        totalProducerErrors = 0;
        totalProducerThroughput = 0;
        totalConsumerThroughput = 0;
        totalConsumerBytesThroughput = 0;
        totalProducerBytesThroughput = 0;
        AtomicInteger running = new AtomicInteger();
        running.set(config.consumerThreads + config.producerThreads);
        ArrayList<Thread> consumerThreads = new ArrayList<>();
        for (int i = 1; i <= config.consumerThreads; i++) {
            ConsumerRunner cr = new ConsumerRunner(running, consumerProps, timeRecorder, warmupTime, config);
            consumerThreads.add(new Thread(cr, "Consumer_" + roundFormat(targetRate) + "_" + i));
        }
        ArrayList<Thread> producerThreads = new ArrayList<>();
        for (int i = 1; i <= config.producerThreads; i++) {
            ProducerRunner pr = new ProducerRunner(running, producerProps, warmupTime, runTime, perProducerMessageRate, config);
            producerThreads.add(new Thread(pr, "Producer_" + (int) targetRate + "_" + i));
        }
        String ps = withS(config.producerThreads, "producer");
        String cs = withS(config.consumerThreads, "consumer");
        log("Starting %s and %s, targetRate %s, warmupTime %ds, runTime %ds", ps, cs, roundFormat(targetRate), warmupTime, runTime);
        consumerThreads.forEach(Thread::start);
        producerThreads.forEach(Thread::start);
        long start = System.currentTimeMillis();
        while (running.get() > 0) {
            sleep(1, null);
            long spentTime = System.currentTimeMillis() - start;
            double progress = spentTime / 1000.0 / (warmupTime + runTime);
            if (progress > 1) {
                break;
            }
        }
        producerThreads.forEach(KafkaE2EBenchmark::join);
        consumerThreads.forEach(KafkaE2EBenchmark::join);
        log("Requested MR: %s %s", roundFormat(targetRate), RATE_MSGS_UNITS);
        log("Producer MR: %s %s", roundFormat(totalProducerThroughput), RATE_MSGS_UNITS);
        log("Consumer MR: %s %s", roundFormat(totalConsumerThroughput), RATE_MSGS_UNITS);
        log("Producer %s %s", roundFormat(totalProducerBytesThroughput / 1024. / 1024.), RATE_MB_UNITS);
        log("Consumer %s %s", roundFormat(totalConsumerBytesThroughput / 1024. / 1024.), RATE_MB_UNITS);
        log("Producer msgs count: %d", totalProducerMsgCount);
        log("Consumer msgs count: %d", totalConsumerMsgCount);
        log("Producer total sent: %s MiB (%d)", roundFormat(totalProducerBytes / 1024. / 1024.), totalProducerBytes);
        log("Consumer total read: %s MiB (%d)", roundFormat(totalConsumerBytes / 1024. / 1024.), totalConsumerBytes);
        log("Producer errors: %d", totalProducerErrors);
        log("Producer time: %s ms", roundFormat(producerTime));
        log("Consumer time: %s ms", roundFormat(consumerTime));
        log("Producer msgs count during warmup: %d", totalProducerMsgCountW);
        log("Consumer msgs count during warmup: %d", totalConsumerMsgCountWarmup);
        RunResult results = new RunResult();
        results.rateUnits = RATE_MSGS_UNITS;
        results.rate = totalProducerThroughput;
        results.time = producerTime;
        results.count = totalConsumerMsgCount;
        results.errors = totalProducerErrors;
        return results;
    }

    @Override
    public BenchmarkConfig getConfig() {
        return config;
    }

    private void setupAdminClientProps(Properties props) {
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-" + System.currentTimeMillis());
    }

    private void setupConsumerProps(Properties props) {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "16777216");
        props.put(ConsumerConfig.SEND_BUFFER_CONFIG, "16777216");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); // ensure we have no temporal batching
    }

    private void setupProducerProps(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // ensure writes are synchronous
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        props.put(ProducerConfig.ACKS_CONFIG, config.producerAcks);
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        if (config.batchSize != -1) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.batchSize);
        }
        if (config.lingerMs != -1) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    class ProducerRunner implements Runnable {
        private Properties producerProps;
        private String topic;
        private double messageRate;
        private AtomicInteger runningProducers;
        private int runTime;
        private int warmupTime;
        private int messageLength;
        private int messageLengthMax;
        private long messageCount = 0;
        private long messageCountW = 0;
        private long messageSize = 0;
        private long errorCount = 0;
        private long timeOffs = System.currentTimeMillis() * NS_IN_MS - System.nanoTime();;
        private Random random = new Random(0); // repeatable pseudo random sequence

        private Callback callback = (RecordMetadata metadata, Exception e) -> {
            if (e == null) {
                messageCount++;
                int s = metadata.serializedValueSize();
                if (s > 0) {
                    messageSize += s;
                }
            } else {
                errorCount++;
            }
        };

        private Callback callbackW = (RecordMetadata metadata, Exception e) -> {
            if (e == null) {
                messageCountW++;
            }
        };

        ProducerRunner(AtomicInteger runningProducers, Properties producerProps, int warmupTime, int runTime, double messageRate, KafkaE2EBenchmarkConfig config) {
            this.runningProducers = runningProducers;
            this.producerProps = producerProps;
            this.warmupTime = warmupTime;
            this.runTime = runTime;
            this.messageRate = messageRate;
            this.topic = config.topic;
            this.messageLength = config.messageLength;
            this.messageLengthMax = config.messageLengthMax > config.messageLength ? config.messageLengthMax : config.messageLength;
        }

        private ProducerRecord<byte[], byte[]> getNextRecord(byte[] spaceBytes, boolean isWarmup) {
            Long recordSendTime = System.nanoTime() + timeOffs;
            int msgLength = messageLength + (messageLengthMax > messageLength ? random.nextInt(messageLengthMax - messageLength) : 0);
            byte[] message = (Boolean.toString(isWarmup) + '-' + recordSendTime.toString() + new String(spaceBytes, 0, msgLength)).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, message);
        }

        private void unthrottled(KafkaProducer<byte[], byte[]> producer, long startTimeMs, long finishTimeMs, byte[] spaceBytes) {
            while (System.currentTimeMillis() < finishTimeMs) {
                boolean isWarmup = System.currentTimeMillis() < startTimeMs;
                producer.send(getNextRecord(spaceBytes, isWarmup), isWarmup ? callbackW : callback);
            }
        }

        private void throttled(KafkaProducer<byte[], byte[]> producer, long startTimeMs, long finishTimeMs, byte[] spaceBytes) {
            long timeChunkSizeMs = 10;
            double messageRateMs = messageRate / (1000.0 / timeChunkSizeMs);
            long timeChunkSizeNs = timeChunkSizeMs * 1000000;
            long timeChunkCurrentNs = 0;
            long timeChunkStartNs = System.nanoTime();
            double messagesSent = 0;
            while (System.currentTimeMillis() < finishTimeMs) {
                int i0 = (int) Math.floor(messagesSent);
                int i1 = (int) Math.floor(messagesSent + messageRateMs);
                for (int i = i0; i < i1; i++) {
                    boolean isWarmup = System.currentTimeMillis() < startTimeMs;
                    producer.send(getNextRecord(spaceBytes, isWarmup), isWarmup ? callbackW : callback);
                }
                messagesSent += messageRateMs;
                timeChunkCurrentNs = System.nanoTime() - timeChunkStartNs;
                if (timeChunkSizeNs - timeChunkCurrentNs > 0) {
                    timeChunkStartNs = timeChunkStartNs + timeChunkSizeNs;
                    SleepTool.sleepUntil(timeChunkSizeNs - timeChunkCurrentNs);
                } else {
                    timeChunkStartNs = System.nanoTime();
                }
            }
        }

        @Override
        public void run() {
            log("%s started", Thread.currentThread().getName());
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
            byte[] spaceBytes = new byte[messageLengthMax];
            Arrays.fill(spaceBytes, (byte) ' ');
            long startTimeMs = System.currentTimeMillis() + warmupTime * 1000L;
            long finishTimeMs = System.currentTimeMillis() + (warmupTime + runTime) * 1000L;
            if (messageRate <= 0) {
                unthrottled(producer, startTimeMs, finishTimeMs, spaceBytes);
            } else {
                throttled(producer, startTimeMs, finishTimeMs, spaceBytes);
            }
            producer.flush();
            producer.close();
            recordFinish(startTimeMs);
            runningProducers.decrementAndGet();
        }

        private void recordFinish(long startTimeMs) {
            long endTimeMs = System.currentTimeMillis();
            double timeDiffS = (endTimeMs - startTimeMs) / 1000.0;
            synchronized (producerLock) {
                if (producerTime < endTimeMs - startTimeMs) {
                    producerTime = endTimeMs - startTimeMs;
                }
                totalProducerMsgCount += messageCount;
                totalProducerMsgCountW += messageCountW;
                totalProducerBytes += messageSize;
                totalProducerErrors += errorCount;
                if (timeDiffS > 0) {
                    totalProducerThroughput += messageCount / timeDiffS;
                    totalProducerBytesThroughput += messageSize / timeDiffS;
                }
            }
            log("%s finished, sent %d messages, time %s", Thread.currentThread().getName(), messageCount, roundFormat(timeDiffS));
        }
    }

    class ConsumerRunner implements Runnable {
        private Properties consumerProps;
        private TimeRecorder timeRecorder;
        private AtomicInteger runningConsumers;
        private String topic;
        private int warmupTime;
        private long pollTimeout;
        private long messageCount = 0;
        private long messageCountWarmup = 0;
        private long messageSize = 0;

        ConsumerRunner(AtomicInteger runningConsumers, Properties consumerProps, TimeRecorder timeRecorder, int warmupTime, KafkaE2EBenchmarkConfig config) {
            this.runningConsumers = runningConsumers;
            this.consumerProps = consumerProps;
            this.timeRecorder = timeRecorder;
            this.warmupTime = warmupTime;
            this.pollTimeout = config.pollTimeout;
            this.topic = config.topic;
        }

        @Override
        public void run() {
            long timeOffs = System.currentTimeMillis() * NS_IN_MS - System.nanoTime();
            log("%s started", Thread.currentThread().getName());
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
            // This will block until a connection comes in
            consumer.subscribe(Collections.singletonList(topic));
            consumer.seekToEnd(Collections.emptyList());
            consumer.poll(Duration.ZERO);
            Duration pollDuration = Duration.ofMillis(pollTimeout);
            long startTimeMs = System.currentTimeMillis() + warmupTime * 1000L;
            long endTimeMs = 0;
            while (true) {
                endTimeMs = System.currentTimeMillis();
                Iterator<ConsumerRecord<byte[], byte[]>> recordIter = consumer.poll(pollDuration).iterator();
                if (!recordIter.hasNext()) {
                    break;
                }
                while (recordIter.hasNext()) {
                    ConsumerRecord<byte[], byte[]> consumerRecord = recordIter.next();
                    long recordRecvTime = System.nanoTime();
                    String read = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                    boolean isWarmup = Boolean.parseBoolean(read.substring(0, read.indexOf('-')));
                    long recordSendTime = Long.parseLong(read.substring(read.indexOf('-') + 1).trim());
                    if (isWarmup) {
                        messageCountWarmup++;
                    } else {
                        if (timeRecorder != null) {
                            timeRecorder.recordTimes("msg", recordSendTime + timeOffs, 0, recordRecvTime, true);
                        }
                        messageCount++;
                        messageSize += consumerRecord.value().length;
                    }
                }
            }
            consumer.close();
            recordFinish(startTimeMs, endTimeMs);
            runningConsumers.decrementAndGet();
        }

        private void recordFinish(long startTimeMs, long endTimeMs) {
            double timeDiffS = (endTimeMs - startTimeMs) / 1000.0;
            synchronized (consumerLock) {
                if (consumerTime < endTimeMs - startTimeMs) {
                    consumerTime = endTimeMs - startTimeMs;
                }
                totalConsumerMsgCount += messageCount;
                totalConsumerMsgCountWarmup += messageCountWarmup;
                totalConsumerBytes += messageSize;
                if (timeDiffS > 0) {
                    totalConsumerThroughput += messageCount / timeDiffS;
                    totalConsumerBytesThroughput += messageSize / timeDiffS;
                }
            }
            log("%s finished, read %d messages, time %s", Thread.currentThread().getName(), messageCount, roundFormat(timeDiffS));
        }
    }
}
