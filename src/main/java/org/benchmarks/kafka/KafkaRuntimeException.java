package org.benchmarks.kafka;

public class KafkaRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KafkaRuntimeException(Exception e) {
        super(e);
    }

    public KafkaRuntimeException(String s, Exception e) {
        super(s, e);
    }
}
