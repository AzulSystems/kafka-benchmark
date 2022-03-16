/*
 * Copyright (c) 2021-2022, Azul Systems
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * * Neither the name of [project] nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 */

package org.tussleframework.kafka;

import static org.tussleframework.tools.FormatTool.roundFormat;

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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
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
import org.tussleframework.Benchmark;
import org.tussleframework.BenchmarkConfig;
import org.tussleframework.RunResult;
import org.tussleframework.TimeRecorder;
import org.tussleframework.tools.ConfigLoader;
import org.tussleframework.tools.SleepTool;

public class KafkaE2EBenchmark implements Benchmark {

    static final Logger logger = Logger.getLogger(KafkaE2EBenchmark.class.getName());
    static final String RATE_MSGS_UNITS = "msg/s";
    static final String RATE_MB_UNITS = "MiB/s";
    static final String TIME_MSGS_UNITS = "ms";

    static final String OP_POLL = "poll";
    static final String OP_SEND = "send";
    static final String OP_END_TO_END = "end-to-end";
    static final String[] operations = {
            OP_POLL, OP_SEND, OP_END_TO_END
    };

    static final long NS_IN_MS = 1000000L;

    private long totalConsumerMsgCountWarmup;
    private long totalConsumerMsgCount;
    private long totalProducerMsgCountW;
    private long totalProducerMsgCount;
    private long totalConsumerBytes;
    private long totalProducerBytes;
    private long totalProducerErrors;
    private long consumerTime;
    private long producerTime;
    private double totalConsumerThroughput;
    private double totalProducerThroughput;
    private double totalConsumerBytesThroughput;
    private double totalProducerBytesThroughput;
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
        deleteTopic(false);
    }

    @Override
    public String getName() {
        return "kafka-e2e-benchmark";
    }

    @Override
    public void reset() {
        if (resetRequired) {
            deleteTopic(true);
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

    public void deleteTopic(boolean wait) {
        log("Deleting topic '" + config.topic + "'...");
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(config.topic));
            result.all().get();
            if (wait && config.waitAfterDeleteTopic > 0) {
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

    public boolean checkTopic() throws InterruptedException {
        ListTopicsResult topics = adminClient.listTopics();
        try {
            Set<String> topicsNames = topics.names().get();
            boolean res = topicsNames.contains(config.topic);
            log("Topic '" + config.topic + "' exists " + res);
            return res;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            throw new KafkaRuntimeException("Failed to ctopic '" + config.topic + "'", e);
        }
    }

    public void createTopic() {
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
        try {
            log("Creating new topic '%s' with %s, replication-factor %d, topic timeout %d", config.topic, withS(config.partitions, "partition"), config.replicationFactor, config.createTopicTimeout);
            CreateTopicsOptions createTopicsOptions = new CreateTopicsOptions();
            if (config.createTopicTimeout > 0) {
                createTopicsOptions.timeoutMs(config.createTopicTimeout);
            }
            CreateTopicsResult result = adminClient.createTopics(newTopics, createTopicsOptions);
            result.all().get();
            log("Topic created: '%s'", config.topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (e instanceof org.apache.kafka.common.errors.TopicExistsException ||
                e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log("Topic created: '%s' - TopicExistsException", config.topic);
                return;
            }
            throw new KafkaRuntimeException("Failed to create topic '" + config.topic + "'", e);
        }
    }

    @Override
    public RunResult run(double targetRate, int warmupTime, int runTime, TimeRecorder timeRecorder) {
        if (timeRecorder != null) {
            for (String op: operations) {
                timeRecorder.startRecording(op, RATE_MSGS_UNITS, TIME_MSGS_UNITS);
            }
        }
        double perProducerMessageRate = targetRate / config.producers;
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
        running.set(config.consumers + config.producers);
        ArrayList<Thread> consumerThreads = new ArrayList<>();
        for (int i = 1; i <= config.consumers; i++) {
            ConsumerRunner cr = new ConsumerRunner(running, consumerProps, timeRecorder, warmupTime, config);
            consumerThreads.add(new Thread(cr, "Consumer_" + roundFormat(targetRate) + "_" + i));
        }
        ArrayList<Thread> producerThreads = new ArrayList<>();
        for (int i = 1; i <= config.producers; i++) {
            ProducerRunner pr = new ProducerRunner(running, producerProps, timeRecorder, warmupTime, runTime, perProducerMessageRate, config);
            producerThreads.add(new Thread(pr, "Producer_" + (int) targetRate + "_" + i));
        }
        String ps = withS(config.producers, "producer");
        String cs = withS(config.consumers, "consumer");
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
        return RunResult.builder()
                .rateUnits(RATE_MSGS_UNITS)
                .rate(totalProducerThroughput)
                .time(producerTime)
                .count(totalConsumerMsgCount)
                .errors(totalProducerErrors).build();
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
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); // ensure no temporal batching
    }

    private void setupProducerProps(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // ensure writes are synchronous
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
        props.put(ProducerConfig.ACKS_CONFIG, config.acks);
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (config.batchSize != -1) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.batchSize);
        }
        if (config.lingerMs != -1) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs);
        }
        if (config.requestTimeoutMs != -1) {
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs);
        }
    }

    class ProducerRunner implements Runnable {
        private AtomicInteger runningProducers;
        private TimeRecorder timeRecorder;
        private Properties producerProps;
        private String topic;
        private double messageRate;
        private int messageLengthMax;
        private int messageLength;
        private int warmupTime;
        private int runTime;
        private long messageCount = 0;
        private long messageCountW = 0;
        private long messageSize = 0;
        private long errorCount = 0;
        private long timeOffs = System.currentTimeMillis() * NS_IN_MS - System.nanoTime();
        private Random random = new Random(0); // repeatable pseudo random sequence
        private char[] spaceChars;

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

        ProducerRunner(AtomicInteger runningProducers, Properties producerProps, TimeRecorder timeRecorder, int warmupTime, int runTime, double messageRate, KafkaE2EBenchmarkConfig config) {
            this.runningProducers = runningProducers;
            this.producerProps = producerProps;
            this.timeRecorder = timeRecorder;
            this.warmupTime = warmupTime;
            this.runTime = runTime;
            this.messageRate = messageRate;
            this.topic = config.topic;
            this.messageLength = config.messageLength;
            this.messageLengthMax = config.messageLengthMax > config.messageLength ? config.messageLengthMax : config.messageLength;
            this.spaceChars = new char[messageLengthMax];
            Arrays.fill(this.spaceChars, ' ');
        }

        private ProducerRecord<byte[], byte[]> getNextRecord(boolean isWarmup, long recordSendTime) {
            int msgLength = messageLength + (messageLengthMax > messageLength ? random.nextInt(messageLengthMax - messageLength) : 0);
            StringBuilder sb = new StringBuilder();
            sb.append(isWarmup).append('-').append(recordSendTime);
            if (msgLength > sb.length()) {
                sb.append(spaceChars, 0, msgLength - sb.length());
            }
            byte[] message = sb.toString().getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, message);
        }

        private void send(KafkaProducer<byte[], byte[]> producer, long startTimeMs) {
            boolean isWarmup = System.currentTimeMillis() < startTimeMs;
            long recordSendStartTime = System.nanoTime() + timeOffs;
            producer.send(getNextRecord(isWarmup, recordSendStartTime), isWarmup ? callbackW : callback);
            long recordSendFinishedTime = System.nanoTime() + timeOffs;
            if (!isWarmup && timeRecorder != null) {
                timeRecorder.recordTimes(OP_SEND, recordSendStartTime, 0, recordSendFinishedTime, true);
            }
        }

        private void unthrottled(KafkaProducer<byte[], byte[]> producer, long startTimeMs, long finishTimeMs) {
            while (System.currentTimeMillis() < finishTimeMs) {
                send(producer, startTimeMs);
            }
        }

        private void throttled(KafkaProducer<byte[], byte[]> producer, long startTimeMs, long finishTimeMs) {
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
                    send(producer, startTimeMs);
                }
                messagesSent += messageRateMs;
                timeChunkCurrentNs = System.nanoTime() - timeChunkStartNs;
                if (timeChunkSizeNs - timeChunkCurrentNs > 0) {
                    timeChunkStartNs = timeChunkStartNs + timeChunkSizeNs;
                    SleepTool.sleep(timeChunkSizeNs - timeChunkCurrentNs);
                } else {
                    timeChunkStartNs = System.nanoTime();
                }
            }
        }

        @Override
        public void run() {
            log("%s started", Thread.currentThread().getName());
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
            long startTimeMs = System.currentTimeMillis() + warmupTime * 1000L;
            long finishTimeMs = System.currentTimeMillis() + (warmupTime + runTime) * 1000L;
            if (messageRate <= 0) {
                unthrottled(producer, startTimeMs, finishTimeMs);
            } else {
                throttled(producer, startTimeMs, finishTimeMs);
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
        private AtomicInteger runningConsumers;
        private TimeRecorder timeRecorder;
        private Properties consumerProps;
        private String topic;
        private long messageCountWarmup = 0;
        private long messageCount = 0;
        private long messageSize = 0;
        private long pollTimeoutMs;
        private int warmupTime;

        ConsumerRunner(AtomicInteger runningConsumers, Properties consumerProps, TimeRecorder timeRecorder, int warmupTime, KafkaE2EBenchmarkConfig config) {
            this.runningConsumers = runningConsumers;
            this.consumerProps = consumerProps;
            this.timeRecorder = timeRecorder;
            this.warmupTime = warmupTime;
            this.pollTimeoutMs = config.pollTimeoutMs;
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
            Duration pollDuration = Duration.ofMillis(pollTimeoutMs);
            long startTimeMs = System.currentTimeMillis() + warmupTime * 1000L;
            long endTimeMs = 0;
            while (true) {
                endTimeMs = System.currentTimeMillis();
                long recordPollStartTime = System.nanoTime() + timeOffs;
                boolean isWarmup = System.currentTimeMillis() < startTimeMs;
                Iterator<ConsumerRecord<byte[], byte[]>> recordIter = consumer.poll(pollDuration).iterator();
                long recordPollFinishTime = System.nanoTime() + timeOffs;
                if (!recordIter.hasNext()) {
                    break;
                }
                if (System.currentTimeMillis() >= startTimeMs && timeRecorder != null) {
                    timeRecorder.recordTimes(OP_POLL, recordPollStartTime, 0, recordPollFinishTime, true);
                }
                while (recordIter.hasNext()) {
                    ConsumerRecord<byte[], byte[]> consumerRecord = recordIter.next();
                    long recordRecvTime = System.nanoTime() + timeOffs;
                    String read = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                    // boolean isWarmup = Boolean.parseBoolean(read.substring(0, read.indexOf('-'))) - using time-based warmup instead
                    long recordSendTime = Long.parseLong(read.substring(read.indexOf('-') + 1).trim());
                    if (isWarmup) {
                        messageCountWarmup++;
                    } else {
                        if (timeRecorder != null) {
                            timeRecorder.recordTimes(OP_END_TO_END, recordSendTime, 0, recordRecvTime, true);
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
