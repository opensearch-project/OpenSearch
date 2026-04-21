/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable request object for creating a local stage execution context.
 * Carries the query identity, serialized fragment, Arrow allocator,
 * downstream sink, and per-child schemas.
 *
 * @opensearch.internal
 */
public final class LocalStageRequest {

    private final String queryId;
    private final int stageId;
    private final byte[] fragmentBytes;
    private final BufferAllocator allocator;
    private final ExchangeSink downstream;
    private final Map<Integer, Schema> childSchemas;

    /**
     * Constructs a local stage request.
     *
     * @param queryId       the query identifier
     * @param stageId       the stage identifier within the query DAG
     * @param fragmentBytes backend-specific serialized plan fragment (e.g. Substrait bytes)
     * @param allocator     Arrow buffer allocator for off-heap memory
     * @param downstream    the sink that receives the finalized output of this local stage
     * @param childSchemas  per-child-stage Arrow schemas, keyed by child stage id
     */
    public LocalStageRequest(
        String queryId,
        int stageId,
        byte[] fragmentBytes,
        BufferAllocator allocator,
        ExchangeSink downstream,
        Map<Integer, Schema> childSchemas
    ) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.fragmentBytes = Arrays.copyOf(fragmentBytes, fragmentBytes.length);
        this.allocator = allocator;
        this.downstream = downstream;
        this.childSchemas = Collections.unmodifiableMap(new HashMap<>(childSchemas));
    }

    /** Returns the query identifier. */
    public String getQueryId() {
        return queryId;
    }

    /** Returns the stage identifier within the query DAG. */
    public int getStageId() {
        return stageId;
    }

    /** Returns a defensive copy of the backend-specific serialized plan fragment bytes. */
    public byte[] getFragmentBytes() {
        return Arrays.copyOf(fragmentBytes, fragmentBytes.length);
    }

    /** Returns the Arrow buffer allocator for off-heap memory. */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /** Returns the downstream sink that receives finalized output. */
    public ExchangeSink getDownstream() {
        return downstream;
    }

    /** Returns an unmodifiable view of the per-child-stage Arrow schemas. */
    public Map<Integer, Schema> getChildSchemas() {
        return childSchemas;
    }
}
