package org.oracle.okafka.clients.producer.internals;

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Header;
import org.oracle.okafka.clients.producer.OKafkaHeaders;

/** Internal validation and conversion shared by both producer send paths. */
public final class MessageExpiration {
    // AQ API 23.3 narrows milliseconds to int before converting to seconds.
    public static final long MAX_TTL_MS = (Integer.MAX_VALUE / 1000L) * 1000L;

    private MessageExpiration() { }

    /** Returns -1 when absent, zero when invalid or no expiration, otherwise rounded milliseconds. */
    public static long timeToLive(Header[] headers) {
        if (headers != null) {
            for (Header header : headers) {
                if (!OKafkaHeaders.TTL_MS.equals(header.key())) {
                    continue;
                }
                byte[] value = header.value();
                if (value == null || value.length != Long.BYTES) {
                    return 0;
                }
                try {
                    return validateAndRound(ByteBuffer.wrap(value).getLong());
                } catch (IllegalArgumentException e) {
                    return 0;
                }
            }
        }
        return -1;
    }

    public static long validateAndRound(long millis) {
        if (millis < 0 || millis > MAX_TTL_MS) {
            throw new IllegalArgumentException("Message TTL must be between 0 and " + MAX_TTL_MS + " ms");
        }
        return millis == 0 ? 0 : ((millis - 1) / 1000 + 1) * 1000;
    }
}
