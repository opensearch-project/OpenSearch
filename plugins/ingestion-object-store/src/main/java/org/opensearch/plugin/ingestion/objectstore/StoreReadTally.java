/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

/**
 * Accumulates the get requests and bytes a shard spends before its {@link ObjectStoreShardConsumer} exists.
 * {@link SliceAssignment} resolves line boundaries through {@link BoundaryResolver} during slice assignment, which
 * happens in {@link ObjectStoreConsumerFactory} before the consumer that owns the {@code max_get_requests} and
 * {@code max_bytes} counters is constructed, so without this tally those reads are charged to nothing and a shard can
 * exceed both caps by one record's worth of bytes per resolved cut before it has read a single record.
 */
final class StoreReadTally {

    private long getRequests;
    private long bytesRead;

    /**
     * Records one get request and the bytes it delivered.
     *
     * @param bytes the number of bytes the request delivered
     */
    void recordRead(final long bytes) {
        getRequests++;
        bytesRead += bytes;
    }

    /**
     * Returns the number of get requests recorded.
     *
     * @return the get request count
     */
    long getRequests() {
        return getRequests;
    }

    /**
     * Returns the number of bytes recorded.
     *
     * @return the byte count
     */
    long bytesRead() {
        return bytesRead;
    }
}
