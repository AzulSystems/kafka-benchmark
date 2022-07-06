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

import org.tussleframework.BenchmarkConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class KafkaE2EBenchmarkConfig extends BenchmarkConfig {
    public KafkaThrottleMode throttleMode;  // use throttling method for reaching target rate  
    public String brokerList = "localhost:9092"; // list of Kafka brokers
    public String topic = "test";           // Kafka topic used for testing
    public String acks = "1";               // ProducerConfig.ACKS_DOC
    public boolean idempotence = false;     // ProducerConfig.ENABLE_IDEMPOTENCE_DOC
    public boolean probeTopics = false;     // perform message probe
    public int topics = 1;                  // Kafka topic number
    public int partitions = 1;              // Kafka topic partitions number 
    public int replicationFactor = 1;       // Kafka topic replication factor
    public int waitAfterDeleteTopic = 3;    // seconds, time to do nothing after topic deletion
    public int messageLength = 1024;        // minimum size of a message in bytes
    public int messageLengthMax = 0;        // maximum size of a message in bytes if > messageLength, else = messageLength
    public int producers = 1;               // number of producers 
    public int consumers = 1;               // number of consumers
    public int pollTimeoutMs = 100;         // consumer poll timeout 
    public int batchSize = -1;              // ProducerConfig.BATCH_SIZE_DOC
    public int lingerMs = -1;               // ProducerConfig.LINGER_MS_DOC
    public int retentionMs = -1;            // ProducerConfig.RETENTION_MS_DOC
    public int retentionBytes = -1;         // ProducerConfig.RETENTION_BYTES_DOC
    public int requestTimeoutMs = -1;       // ProducerConfig.REQUEST_TIMEOUT_MS_DOC

    @Override
    public void validate(boolean runMode) {
        super.validate(runMode);
        name = "kafka-e2e-benchmark";
        rateUnits = "msg/s";
        timeUnits = "ms";
        if (topics < 1) {
            throw new IllegalArgumentException(String.format("Invalid topics(%d) - should be positive", topics));
        }
        if (partitions < 1) {
            throw new IllegalArgumentException(String.format("Invalid partitions(%d) - should be positive", partitions));
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException(String.format("Invalid replicationFactor(%d) - should be positive", replicationFactor));
        }
        if (producers < 1) {
            throw new IllegalArgumentException(String.format("Invalid producers(%d) - should be positive", producers));
        }
        if (consumers < 1) {
            throw new IllegalArgumentException(String.format("Invalid consumers(%d) - should be positive", consumers));
        }
    }
}
