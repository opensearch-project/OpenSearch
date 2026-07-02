/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

/**
 * Transforms raw ingestion message bytes before they are handed to the indexing pipeline.
 *
 * <p>Implementations can strip wire-format headers, decode binary formats (e.g. Avro, Protobuf),
 * and convert the result to the JSON bytes the downstream mapper expects.
 *
 * <p>The default no-op implementation is {@link #PASSTHROUGH}, which returns raw bytes unchanged.
 * Use it when no payload transformation is required.
 *
 * <p>Implementations must be thread-safe: {@link #decode} may be called concurrently from
 * multiple processor threads via lazy evaluation in the message layer.
 */
@FunctionalInterface
public interface PayloadDecoder {

    /** Returns raw bytes unchanged — used when no payload transformation is configured. */
    PayloadDecoder PASSTHROUGH = raw -> raw;

    /**
     * Transforms {@code raw} message bytes and returns the decoded result.
     *
     * @param raw the raw bytes from the ingestion source; may be {@code null} for tombstone records
     * @return decoded bytes for the indexing pipeline, or {@code null} to signal a tombstone
     */
    byte[] decode(byte[] raw);
}
