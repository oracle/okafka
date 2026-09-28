package org.oracle.okafka.clients.producer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.oracle.okafka.clients.producer.internals.MessageExpiration;

/** Helpers for per-record TxEventQ options carried in standard Kafka headers. */
public final class OKafkaHeaders {
    /** Eight-byte, big-endian TTL in milliseconds; zero means no message expiration. */
    public static final String TTL_MS = "x-okafka-ttl-ms";

    private OKafkaHeaders() { }

    /**
     * Creates a relative message lifetime header. Positive lifetimes are rounded up
     * to whole seconds when sent to AQ. This does not change topic retention.
     *
     * @param lifetime zero for no expiration, or a positive duration
     * @return a header to add to a ProducerRecord
     * @throws IllegalArgumentException if negative or outside the AQ client range
     */
    public static Header timeToLive(Duration lifetime) {
        Objects.requireNonNull(lifetime, "lifetime");
        if (lifetime.isNegative()) {
            throw new IllegalArgumentException("Message TTL must not be negative");
        }
        long millis;
        try {
            millis = lifetime.toMillis();
            if (lifetime.getNano() % 1_000_000 != 0) {
                millis = Math.addExact(millis, 1);
            }
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Message TTL is too large", e);
        }
        MessageExpiration.validateAndRound(millis);
        return new RecordHeader(TTL_MS, ByteBuffer.allocate(Long.BYTES).putLong(millis).array());
    }

    /**
     * Returns the TTL in milliseconds encoded in a consumed record's headers.
     * A value of -1 means the record has no TTL header; zero means the producer
     * explicitly requested no expiration.
     *
     * @param headers headers from a consumed record
     * @return the raw TTL value encoded by the producer, or -1 when absent
     * @throws IllegalArgumentException if the TTL header is malformed or duplicated
     */
    public static long timeToLive(Headers headers) {
        Objects.requireNonNull(headers, "headers");
        long ttl = -1;
        for (Header header : headers) {
            if (!TTL_MS.equals(header.key())) {
                continue;
            }
            if (ttl != -1) {
                throw new IllegalArgumentException("Duplicate " + TTL_MS + " header");
            }
            byte[] value = header.value();
            if (value == null || value.length != Long.BYTES) {
                throw new IllegalArgumentException(TTL_MS + " must contain an eight-byte big-endian integer");
            }
            ttl = ByteBuffer.wrap(value).getLong();
            if (ttl < 0 || ttl > MessageExpiration.MAX_TTL_MS) {
                throw new IllegalArgumentException("Message TTL must be between 0 and "
                        + MessageExpiration.MAX_TTL_MS + " ms");
            }
        }
        return ttl;
    }
}
