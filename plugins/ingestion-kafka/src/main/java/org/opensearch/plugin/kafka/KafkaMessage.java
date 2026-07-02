/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.common.Nullable;
import org.opensearch.index.Message;

/**
 * Kafka message.
 *
 * <p>When constructed with a {@link KafkaPayloadDecoder}, the payload is decoded lazily on the
 * first call to {@link #getPayload()}. This means decoding errors surface in the processor rather
 * than in the consumer's fetch loop, allowing the configured DROP/BLOCK error strategy to apply.
 */
public class KafkaMessage implements Message<byte[]> {
    private final byte[] key;
    private final byte[] rawPayload;
    private final Long timestamp;
    private final KafkaPayloadDecoder decoder;

    private byte[] decodedPayload;
    private volatile boolean decoded;

    /**
     * Constructor for pre-decoded payloads (tests and PASSTHROUGH path).
     */
    public KafkaMessage(@Nullable byte[] key, byte[] payload, Long timestamp) {
        this(key, payload, timestamp, KafkaPayloadDecoder.PASSTHROUGH);
        this.decodedPayload = payload;
        this.decoded = true;
    }

    /**
     * Constructor for lazy decoding. {@link #getPayload()} will invoke {@code decoder.decode()}
     * on the first call, allowing decode errors to propagate to the processor.
     */
    public KafkaMessage(@Nullable byte[] key, byte[] rawPayload, Long timestamp, KafkaPayloadDecoder decoder) {
        this.key = key;
        this.rawPayload = rawPayload;
        this.timestamp = timestamp;
        this.decoder = decoder;
        this.decoded = false;
    }

    /**
     * Get the key of the message
     * @return the key of the message
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Returns the decoded payload. Decoding is performed lazily on the first call.
     * If the decoder throws, the exception propagates to the caller so the
     * configured DROP/BLOCK error strategy can handle it.
     */
    @Override
    public byte[] getPayload() {
        if (!decoded) {
            decodedPayload = decoder.decode(rawPayload);
            decoded = true;
        }
        return decodedPayload;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}
