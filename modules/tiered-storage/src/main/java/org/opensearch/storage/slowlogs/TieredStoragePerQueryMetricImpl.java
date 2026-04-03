/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Implementation for collecting tiered storage metrics at per-query level.
 * Tracks file cache hits/misses, prefetch operations, and read-ahead operations per file.
 * Full metric collection logic, toXContent serialization, and inner stat classes
 * will be added in the implementation PR.
 */
public class TieredStoragePerQueryMetricImpl implements TieredStoragePerQueryMetric, ToXContentObject {

    /** Constructs a new TieredStoragePerQueryMetricImpl. */
    public TieredStoragePerQueryMetricImpl() {}

    @Override
    public void recordFileAccess(String blockFileName, boolean hit) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void recordPrefetch(String fileName, int blockId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void recordReadAhead(String fileName, int blockId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void recordEndTime() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getParentTaskId() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getShardId() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long ramBytesUsed() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
