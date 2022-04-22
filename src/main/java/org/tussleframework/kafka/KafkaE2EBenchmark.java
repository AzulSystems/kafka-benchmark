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

import static org.tussleframework.tools.FormatTool.NS_IN_MS;
import static org.tussleframework.tools.FormatTool.NS_IN_S;
import static org.tussleframework.tools.FormatTool.roundFormat;
import static org.tussleframework.tools.FormatTool.withS;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.tussleframework.AbstractConfig;
import org.tussleframework.Benchmark;
import org.tussleframework.RunResult;
import org.tussleframework.TimeRecorder;
import org.tussleframework.TussleException;
import org.tussleframework.tools.ConfigLoader;
import org.tussleframework.tools.FormatTool;
import org.tussleframework.tools.SleepTool;

enum KafkaOp {
    POLL("poll"),
    SEND("send"),
    END_TO_END("end-to-end");

    final String value;

    KafkaOp(String value) {
        this.value = value;
    }
}

public class KafkaE2EBenchmark implements Benchmark {

    static final Logger logger = Logger.getLogger(KafkaE2EBenchmark.class.getName());
    static final String RATE_MSGS_UNITS = "msg/s";
    static final String RATE_MB_UNITS = "MiB/s";
    static final String TIME_MSGS_UNITS = "ms";
    static final long NANO_TIME_OFFSET = System.currentTimeMillis() * NS_IN_MS - System.nanoTime();

    class MsgCounter {
        long errors;
        long msgCount;
        long totalTime;
        long msgBytes;
        double bytesThroughput;
        double msgThroughput;

        synchronized long getCount() {
            return msgCount;
        }

        synchronized void add(long count, long size, long errs) {
            msgCount += count;
            if (size > 0) {
                msgBytes += size;
            }
            errors += errs;
        }

        synchronized void accumulate(String name, long startTimeMs, long finishTimeMs, MsgCounter msgCounter) {
            msgCount += msgCounter.msgCount;
            msgBytes += msgCounter.msgBytes;
            errors += msgCounter.errors;
            double timeDiffSec = (finishTimeMs - startTimeMs) / 1000.0;
            if (totalTime < finishTimeMs - startTimeMs) {
                totalTime = finishTimeMs - startTimeMs;
            }
            if (timeDiffSec > 0) {
                msgThroughput += msgCounter.msgCount / timeDiffSec;
                bytesThroughput += msgCounter.msgBytes / timeDiffSec;
            }
            log("%s, %d messages, time %s s", name, msgCounter.msgCount, roundFormat(timeDiffSec));
        }

        void print(String name) {
            log("%s msgs rate: %s %s", name, roundFormat(msgThroughput), RATE_MSGS_UNITS);
            log("%s xfer rate: %s %s", name, roundFormat(bytesThroughput / 1024. / 1024.), RATE_MB_UNITS);
            log("%s msgs count: %d", name, msgCount);
            log("%s xfer size: %s MiB (%d)", name, roundFormat(msgBytes / 1024. / 1024.), msgBytes);
            log("%s time: %s ms", name, roundFormat(totalTime));
            log("%s errors: %d", name, errors);
        }
    }

    private KafkaE2EBenchmarkConfig config;
    private MsgCounter consumerWarmupCounter;
    private MsgCounter producerWarmupCounter;
    private MsgCounter consumerCounter;
    private MsgCounter producerCounter;
    private Properties consumerProps;
    private Properties producerProps;
    private AdminClient adminClient;
    private volatile boolean running;
    private boolean resetRequired = true;

    public static void log(String format, Object... args) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info(String.format("[KafkaE2EBenchmark] %s", String.format(format, args)));
        }
    }

    public KafkaE2EBenchmark() {
    }

    public KafkaE2EBenchmark(String[] args) throws TussleException {
        init(args);
    }

    public KafkaE2EBenchmark(KafkaE2EBenchmarkConfig config) {
        this.config = config;
        initProps();
    }

    @Override
    public void init(String[] args) throws TussleException {
        config = ConfigLoader.loadConfig(args, true, KafkaE2EBenchmarkConfig.class);
        initProps();
    }

    @Override
    public void cleanup() {
        deleteTopics(false);
    }

    @Override
    public String getName() {
        return "kafka-e2e-benchmark";
    }

    @Override
    public void reset() {
        if (resetRequired) {
            deleteTopics(true);
            createTopics();
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
            log("Sleeping %s - %s", withS(s, "second"), msg);
        }
        try {
            Thread.sleep(s * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public List<String> getTopics() {
        ArrayList<String> topicNames = new ArrayList<>();
        for (int i = 1; i <= config.topics; i++) {
            topicNames.add(config.topic + "_" + i);
        }
        return topicNames;
    }

    public void deleteTopics(boolean wait) {
        Collection<String> topics = getTopics();
        String topicsJoin = FormatTool.join(topics, ", ");
        log("Deleting topic(s): %s...", topicsJoin);
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(topics);
            result.all().get();
            if (wait && config.waitAfterDeleteTopic > 0) {
                sleep(config.waitAfterDeleteTopic, "waiting after topic deletion");
            }
            log("Topic deleted '%s'", topicsJoin);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (e instanceof UnknownTopicOrPartitionException) {
                log("Cannot delete existing topic(s): %s - %s", topicsJoin, e);
                return;
            }
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                log("Cannot delete existing topic(s): %s - %s", topicsJoin, e.getCause());
                return;
            }
            throw new KafkaRuntimeException("Failed to delete topic(s): " + topicsJoin, e);
        }
    }

    public void createTopics() {
        Map<String, String> configs = new HashMap<>();
        if (config.retentionMs > 0) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.retentionMs));
        }
        if (config.retentionBytes > 0) {
            configs.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(config.retentionBytes));
        }
        ArrayList<NewTopic> newTopics = new ArrayList<>();
        Collection<String> topics = getTopics();
        String topicsJoin = FormatTool.join(topics, ", ");
        topics.forEach(topic -> {
            NewTopic newTopic = new NewTopic(topic, config.partitions, (short) config.replicationFactor);
            if (configs.size() > 0) {
                newTopic.configs(configs);
            }
            newTopics.add(newTopic);
        });
        try {
            log("Creating %s: %s, %s, replication-factor %d", withS(topics.size(), "new topic"), topicsJoin, withS(config.partitions, "partition"), config.replicationFactor);
            CreateTopicsOptions createTopicsOptions = new CreateTopicsOptions();
            CreateTopicsResult result = adminClient.createTopics(newTopics, createTopicsOptions);
            result.all().get();
            log("%s created: %s", withS(topics.size(), "new topic"), topicsJoin);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (e instanceof org.apache.kafka.common.errors.TopicExistsException || e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log("%s created: '%s' - TopicExistsException", withS(topics.size(), "topic"), topicsJoin);
            } else {
                throw new KafkaRuntimeException("Failed to create topic(s): " + topicsJoin, e);
            }
        }
        if (config.probeTopics) {
            probeTopics();
        }
    }

    public void probeTopics() {
        getTopics().forEach(this::probeTopic);
    }

    public void probeTopic(String topic) {
        log("Probing Kafka topic '%s'...", topic);
        byte[] message = { 'Z' };
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps); KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            log("Sending probe message...");
            producer.send(new ProducerRecord<>(topic, message)).get();
            producer.flush();
            Collection<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(p -> topicPartitions.add(new TopicPartition(p.topic(), p.partition())));
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);
            for (int i = 1; i <= 300; i++) {
                log("Polling probe message (#%d)...", i);
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(config.pollTimeoutMs));
                if (records.iterator().hasNext()) {
                    log("Probe message received (%d)!", records.count());
                    break;
                }
                sleep(1, "waiting after topic poll");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new KafkaRuntimeException("Failed to probe topic '" + topic + "'", e);
        }
    }

    @Override
    public RunResult run(double targetRate, int warmupTime, int runTime, TimeRecorder timeRecorder) {
        double perProducerMessageRate = targetRate / config.producers;
        running = true;
        resetRequired = true;
        consumerCounter = new MsgCounter();
        producerCounter = new MsgCounter();
        consumerWarmupCounter = new MsgCounter();
        producerWarmupCounter = new MsgCounter();
        List<String> topics = getTopics();
        AtomicInteger runningCount = new AtomicInteger();
        runningCount.set(config.consumers + config.producers);
        ArrayList<Thread> consumerThreads = new ArrayList<>();
        for (int i = 1; i <= config.consumers; i++) {
            ConsumerRunner cr = new ConsumerRunner(runningCount, timeRecorder, warmupTime, topics.get((i - 1) % topics.size()));
            consumerThreads.add(new Thread(cr, "Consumer_" + roundFormat(targetRate) + "_" + i));
        }
        ArrayList<Thread> producerThreads = new ArrayList<>();
        for (int i = 1; i <= config.producers; i++) {
            ProducerRunner pr = new ProducerRunner(i, runningCount, timeRecorder, warmupTime, runTime, perProducerMessageRate, topics.get((i - 1) % topics.size()));
            producerThreads.add(new Thread(pr, "Producer_" + (int) targetRate + "_" + i));
        }
        String ps = withS(config.producers, "producer");
        String cs = withS(config.consumers, "consumer");
        log("Starting %s and %s, targetRate %s, warmupTime %ds, runTime %ds", ps, cs, roundFormat(targetRate), warmupTime, runTime);
        consumerThreads.forEach(Thread::start);
        producerThreads.forEach(Thread::start);
        SleepTool.sleep(warmupTime * NS_IN_S);
        if (timeRecorder != null) {
            for (KafkaOp op : KafkaOp.values()) {
                timeRecorder.startRecording(op.value, RATE_MSGS_UNITS, TIME_MSGS_UNITS);
            }
        }
        SleepTool.sleep(runTime * NS_IN_S);
        producerThreads.forEach(KafkaE2EBenchmark::join);
        if (timeRecorder != null) {
            timeRecorder.stopRecording();
        }
        running = false;
        consumerThreads.forEach(KafkaE2EBenchmark::join);
        log("Requested MR: %s %s", roundFormat(targetRate), RATE_MSGS_UNITS);
        if (producerWarmupCounter.getCount() > 0) {
            producerWarmupCounter.print("Producer (warmup)");
            consumerWarmupCounter.print("Consumer (warmup)");
        }
        producerCounter.print("Producer");
        consumerCounter.print("Consumer");
        return RunResult.builder()
                .rateUnits(RATE_MSGS_UNITS)
                .time(producerCounter.totalTime)
                .rate(producerCounter.msgThroughput)
                .count(consumerCounter.msgCount)
                .errors(producerCounter.errors)
                .build();
    }

    @Override
    public AbstractConfig getConfig() {
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
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.idempotence);
    }

    class ProducerRunner implements Runnable {
        private AtomicInteger runningProducers;
        private KafkaThrottleMode throttleMode;
        private TimeRecorder timeRecorder;
        private MsgCounter msgCounter = new MsgCounter();
        private MsgCounter warmupMsgCounter = new MsgCounter();
        private Random random = new Random(0); // repeatable pseudo random sequence
        private String topic;
        private double messageRate;
        private int messageLengthMax;
        private int messageLength;
        private int warmupTime;
        private int producerId;
        private int runTime;
        private long warmupPrintTime;
        private char[] spaceChars;

        ProducerRunner(int producerId, AtomicInteger runningProducers, TimeRecorder timeRecorder, int warmupTime, int runTime, double messageRate, String topic) {
            this.runningProducers = runningProducers;
            this.timeRecorder = timeRecorder;
            this.messageRate = messageRate;
            this.warmupTime = warmupTime;
            this.producerId = producerId;
            this.runTime = runTime;
            this.topic = topic;
            this.throttleMode = config.throttleMode;
            this.messageLength = config.messageLength;
            this.messageLengthMax = config.messageLengthMax > config.messageLength ? config.messageLengthMax : config.messageLength;
            this.spaceChars = new char[messageLengthMax];
            Arrays.fill(this.spaceChars, ' ');
        }

        private ProducerRecord<byte[], byte[]> getNextRecord(long recordSendTime, long recordIntendedSendTime) {
            int msgLength = messageLength + (messageLengthMax > messageLength ? random.nextInt(messageLengthMax - messageLength) : 0);
            StringBuilder sb = new StringBuilder();
            sb.append(recordSendTime).append('-').append(recordIntendedSendTime);
            if (msgLength > sb.length()) {
                sb.append(spaceChars, 0, msgLength - sb.length());
            }
            byte[] message = sb.toString().getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, message);
        }

        private void send(KafkaProducer<byte[], byte[]> producer, boolean isWarmup, long intendedNextStartTime) {
            long recordSendStartTime = System.nanoTime() + NANO_TIME_OFFSET;
            if (isWarmup && warmupPrintTime < recordSendStartTime && producerId == 1) {
                log("warmup...");
                warmupPrintTime = recordSendStartTime + NS_IN_S * 5;
            }
            producer.send(getNextRecord(recordSendStartTime, intendedNextStartTime), (RecordMetadata metadata, Exception e) -> {
                long recordSendFinishedTime = System.nanoTime() + NANO_TIME_OFFSET;
                if (e == null) {
                    int s = metadata.serializedValueSize();
                    (isWarmup ? warmupMsgCounter : msgCounter).add(1, s, 0);
                    if (!isWarmup && timeRecorder != null) {
                        timeRecorder.recordTimes(KafkaOp.SEND.value, recordSendStartTime, intendedNextStartTime, recordSendFinishedTime, 1, true);
                    }
                } else {
                    (isWarmup ? warmupMsgCounter : msgCounter).add(0, 0, 1);
                }
            });
        }

        private void unthrottled(KafkaProducer<byte[], byte[]> producer, long startPostWarmupTimeMs, long finishTimeMs) {
            while (System.currentTimeMillis() < finishTimeMs) {
                boolean isWarmup = System.currentTimeMillis() < startPostWarmupTimeMs;
                send(producer, isWarmup, 0);
            }
        }

        private void throttleEven(KafkaProducer<byte[], byte[]> producer, long startPostWarmupTimeMs, long finishTimeMs) {
            long opIndex = 0;
            long delayBetweenOps = (long) (NS_IN_S / messageRate);
            long startRunTime = System.nanoTime();
            while (System.currentTimeMillis() < finishTimeMs) {
                boolean isWarmup = System.currentTimeMillis() < startPostWarmupTimeMs;
                long intendedNextStartTime = (startRunTime + opIndex * delayBetweenOps);
                SleepTool.sleepUntil(intendedNextStartTime);
                send(producer, isWarmup, isWarmup ? 0 : intendedNextStartTime + NANO_TIME_OFFSET);
                opIndex++;
            }
        }

        private void throttleInt(KafkaProducer<byte[], byte[]> producer, long startPostWarmupTimeMs, long finishTimeMs) {
            long timeChunkSizeMs = 10;
            long timeChunkSizeNs = timeChunkSizeMs * 1000000;
            long timeChunkCurrentNs = 0;
            long timeChunkStartNs = System.nanoTime();
            double messageRateMs = messageRate / (1000.0 / timeChunkSizeMs);
            double messagesSent = 0;
            while (System.currentTimeMillis() < finishTimeMs) {
                int i0 = (int) Math.floor(messagesSent);
                int i1 = (int) Math.floor(messagesSent + messageRateMs);
                for (int i = i0; i < i1; i++) {
                    boolean isWarmup = System.currentTimeMillis() < startPostWarmupTimeMs;
                    send(producer, isWarmup, 0);
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
            log("%s started on topic '%s'", Thread.currentThread().getName(), topic);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
            long startWarmupTimeMs = System.currentTimeMillis();
            long startPostWarmupTimeMs = startWarmupTimeMs + warmupTime * 1000L;
            long finishTimeMs = startWarmupTimeMs + (warmupTime + runTime) * 1000L;
            if (messageRate <= 0) {
                unthrottled(producer, startPostWarmupTimeMs, finishTimeMs);
            } else if (throttleMode == KafkaThrottleMode.INT) {
                throttleInt(producer, startPostWarmupTimeMs, finishTimeMs);
            } else {
                throttleEven(producer, startPostWarmupTimeMs, finishTimeMs);
            }
            producer.flush();
            producer.close();
            producerWarmupCounter.accumulate(Thread.currentThread().getName() + " (warmup)", startWarmupTimeMs, startPostWarmupTimeMs, warmupMsgCounter);
            producerCounter.accumulate(Thread.currentThread().getName(), startPostWarmupTimeMs, finishTimeMs, msgCounter);
            runningProducers.decrementAndGet();
        }
    }

    class ConsumerRunner implements Runnable {
        private AtomicInteger runningConsumers;
        private TimeRecorder timeRecorder;
        private MsgCounter msgCounter = new MsgCounter();
        private MsgCounter warmupMsgCounter = new MsgCounter();
        private String topic;
        private int warmupTime;

        ConsumerRunner(AtomicInteger runningConsumers, TimeRecorder timeRecorder, int warmupTime, String topic) {
            this.runningConsumers = runningConsumers;
            this.timeRecorder = timeRecorder;
            this.warmupTime = warmupTime;
            this.topic = topic;
        }

        @Override
        public void run() {
            log("%s started on topic '%s'", Thread.currentThread().getName(), topic);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(topic));
            Duration pollDuration = Duration.ofMillis(config.pollTimeoutMs);
            long startWarmupTimeMs = System.currentTimeMillis();
            long startPostWarmupTimeMs = startWarmupTimeMs + warmupTime * 1000L;
            while (running) {
                long recordPollStartTime = System.nanoTime() + NANO_TIME_OFFSET;
                boolean isWarmup = System.currentTimeMillis() < startPostWarmupTimeMs;
                ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
                long recordPollFinishTime = System.nanoTime() + NANO_TIME_OFFSET;
                long count = 0;
                for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                    long recordRecvTime = System.nanoTime() + NANO_TIME_OFFSET;
                    String read = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                    long recordSendTime = Long.parseLong(read.substring(0, read.indexOf('-')).trim());
                    long recordIntendedSendTime = Long.parseLong(read.substring(read.indexOf('-') + 1).trim());
                    if (isWarmup) {
                        warmupMsgCounter.add(1, consumerRecord.value().length, 0);
                    } else {
                        count++;
                        if (timeRecorder != null) {
                            timeRecorder.recordTimes(KafkaOp.END_TO_END.value, recordSendTime, recordIntendedSendTime, recordRecvTime, 1, true);
                        }
                        msgCounter.add(1, consumerRecord.value().length, 0);
                    }
                }
                if (System.currentTimeMillis() >= startPostWarmupTimeMs && timeRecorder != null) {
                    timeRecorder.recordTimes(KafkaOp.POLL.value, recordPollStartTime, 0, recordPollFinishTime, count, true);
                }
            }
            consumer.close();
            long finishTimeMs = System.currentTimeMillis();
            consumerWarmupCounter.accumulate(Thread.currentThread().getName() + " (warmup)", startWarmupTimeMs, startPostWarmupTimeMs, warmupMsgCounter);
            consumerCounter.accumulate(Thread.currentThread().getName(), startPostWarmupTimeMs, finishTimeMs, msgCounter);
            runningConsumers.decrementAndGet();
        }
    }
}
