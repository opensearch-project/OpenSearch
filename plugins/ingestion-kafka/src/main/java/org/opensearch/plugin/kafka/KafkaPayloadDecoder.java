/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

/**
 * Strategy for transforming raw Kafka message bytes before they are wrapped in a {@link KafkaMessage}.
 *
 * <p>Implementations can strip wire-format headers, decode binary formats (Avro, Protobuf, …),
 * and convert the result to whatever byte representation the downstream mapper expects.
 *
 * <p>The default no-op implementation is {@link #PASSTHROUGH}, which returns the raw bytes unchanged.
 */
@FunctionalInterface
public interface KafkaPayloadDecoder {

    /** Returns raw bytes unchanged — used when no payload transformation is configured. */
    KafkaPayloadDecoder PASSTHROUGH = raw -> raw;

    /**
     * Transforms {@code raw} Kafka message bytes and returns the result.
     *
     * @param raw the raw bytes from the Kafka {@code ConsumerRecord} value
     * @return transformed bytes to store in the {@link KafkaMessage}
     */
    byte[] decode(byte[] raw);
}
