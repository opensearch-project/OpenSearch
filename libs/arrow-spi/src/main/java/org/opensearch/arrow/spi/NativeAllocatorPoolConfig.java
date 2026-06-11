/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

/**
 * Pool name constants and setting keys for native allocator pools.
 *
 * <p>Each pool has a min (guaranteed reservation) and max (burst limit).
 * The rebalancer ensures every pool can always allocate up to its min,
 * and distributes unused capacity up to each pool's max.
 *
 * <p>Limits are provided via {@code opensearch.yml} or the cluster settings API.
 * The setting keys follow the pattern
 * {@code native.allocator.pool.<name>.min} and {@code native.allocator.pool.<name>.max}.
 *
 * <p>This class is Arrow-agnostic — it defines the logical pool topology
 * without referencing any Arrow classes.
 *
 * @opensearch.api
 */
public final class NativeAllocatorPoolConfig {

    /** Pool name for Arrow Flight RPC memory. */
    public static final String POOL_FLIGHT = "flight";
    /** Pool name for ingest pipeline memory. */
    public static final String POOL_INGEST = "ingest";
    /** Pool name for query-execution memory (analytics-engine fragments and per-query allocators). */
    public static final String POOL_QUERY = "query";

    /** Setting key for the Flight pool minimum. */
    public static final String SETTING_FLIGHT_MIN = "native.allocator.pool.flight.min";
    /** Setting key for the Flight pool maximum. */
    public static final String SETTING_FLIGHT_MAX = "native.allocator.pool.flight.max";
    /** Setting key for the ingest pool minimum. */
    public static final String SETTING_INGEST_MIN = "native.allocator.pool.ingest.min";
    /** Setting key for the ingest pool maximum. */
    public static final String SETTING_INGEST_MAX = "native.allocator.pool.ingest.max";
    /** Setting key for the query pool minimum. */
    public static final String SETTING_QUERY_MIN = "native.allocator.pool.query.min";
    /** Setting key for the query pool maximum. */
    public static final String SETTING_QUERY_MAX = "native.allocator.pool.query.max";

    private NativeAllocatorPoolConfig() {}
}
