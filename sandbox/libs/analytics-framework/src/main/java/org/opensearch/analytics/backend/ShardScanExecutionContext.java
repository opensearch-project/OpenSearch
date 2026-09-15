/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.tasks.Task;

/**
 * Execution context carrying reader and plan state through
 * the query execution lifecycle.
 *
 * @opensearch.internal
 */
public class ShardScanExecutionContext implements CommonExecutionContext {

    private final String tableName;
    private final Reader reader;
    private final Task task;
    private byte[] fragmentBytes;
    private BufferAllocator allocator;
    private BufferAllocator importStagingAllocator;
    private MapperService mapperService;
    private IndexSettings indexSettings;
    private NamedWriteableRegistry namedWriteableRegistry;
    private ShuffleBufferRegistry shuffleBufferRegistry;
    private QueryCache queryCache;
    private QueryCachingPolicy queryCachingPolicy;
    private ShardId shardId;
    private boolean hasPartialAggregate;
    private DelegationThreadTracker delegationThreadTracker;

    /**
     * Constructs an execution context.
     * @param tableName the target table name
     * @param task the transport-created task for this fragment execution
     * @param reader the data-format aware reader
     */
    public ShardScanExecutionContext(String tableName, Task task, Reader reader) {
        this.tableName = tableName;
        this.task = task;
        this.reader = reader;
    }

    /** Returns the transport-created task for this fragment execution. */
    public Task getTask() {
        return task;
    }

    /** Returns the target table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the data-format aware reader. */
    public Reader getReader() {
        return reader;
    }

    /** Returns the backend-specific serialized plan fragment bytes, or null if not set. */
    public byte[] getFragmentBytes() {
        return fragmentBytes;
    }

    /** Sets the backend-specific serialized plan fragment bytes. */
    public void setFragmentBytes(byte[] fragmentBytes) {
        this.fragmentBytes = fragmentBytes;
    }

    /** Returns the caller-provided allocator for producing Arrow result buffers. */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /** Sets the caller-provided allocator. The caller owns its lifecycle; the engine must not close it. */
    public void setAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Returns the allocator Arrow C Data Interface imports are staged on, or null if the caller did not
     * set one. Distinct from {@link #getAllocator()} on two properties a batch import depends on:
     * <ul>
     *   <li><b>Unbounded, parented at the root.</b> {@code Data#importIntoVectorSchemaRoot} charges each
     *       buffer as it walks the array; a target that fills part-way through throws, and arrow-java
     *       (&le; 18.1.0) retains the imported array before the throwing {@code wrapForeignAllocation}
     *       without rolling back, so the C Data release callback never fires and the whole batch leaks in
     *       the producer's native allocator — invisible to the JVM heap and to Java Arrow accounting.</li>
     *   <li><b>Node-scoped, never closed per request.</b> The Flight transport builds its reused stream
     *       root on the FIRST emitted batch's vector allocator and charges that same allocator for every
     *       later batch ({@code FlightServerChannel#transferIntoStreamRoot}: "The producer's allocator must
     *       be long-lived (not closed per-request)"), and frees it asynchronously with its own channel.</li>
     * </ul>
     * Caller-owned: the engine imports onto it and must never close it.
     */
    public BufferAllocator getImportStagingAllocator() {
        return importStagingAllocator;
    }

    /** Sets the node-scoped import staging allocator. The caller owns its lifecycle. */
    public void setImportStagingAllocator(BufferAllocator importStagingAllocator) {
        this.importStagingAllocator = importStagingAllocator;
    }

    /** Returns the shard's mapper service for field type resolution. */
    public MapperService getMapperService() {
        return mapperService;
    }

    /** Sets the shard's mapper service. */
    public void setMapperService(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    /** Returns the shard's index settings. */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /** Sets the shard's index settings. */
    public void setIndexSettings(IndexSettings indexSettings) {
        this.indexSettings = indexSettings;
    }

    /** Returns the NamedWriteableRegistry for deserializing delegated expressions. */
    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    /** Sets the NamedWriteableRegistry. */
    public void setNamedWriteableRegistry(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    /**
     * Returns the per-node shuffle buffer registry. Non-null when the data node hosts
     * hash-shuffle workers (analytics-engine binds {@code ShuffleBufferManager} as singleton);
     * null when no such backend is registered. Hash-shuffle scan handlers consult this to
     * locate the buffer for the {@code (queryId, stageId, partitionIndex)} bucket the producers
     * are filling. Other handlers (broadcast, scan, partial-aggregate) ignore it.
     */
    public ShuffleBufferRegistry getShuffleBufferRegistry() {
        return shuffleBufferRegistry;
    }

    /** Sets the per-node shuffle buffer registry. Called once per fragment by
     *  {@code AnalyticsSearchService} when building the context. */
    public void setShuffleBufferRegistry(ShuffleBufferRegistry shuffleBufferRegistry) {
        this.shuffleBufferRegistry = shuffleBufferRegistry;
    }

    /** Returns the node-level query cache for Lucene filter delegation. */
    public QueryCache getQueryCache() {
        return queryCache;
    }

    /** Sets the node-level query cache. */
    public void setQueryCache(QueryCache queryCache) {
        this.queryCache = queryCache;
    }

    /** Returns the query caching policy for Lucene filter delegation. */
    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /** Sets the query caching policy. */
    public void setQueryCachingPolicy(QueryCachingPolicy queryCachingPolicy) {
        this.queryCachingPolicy = queryCachingPolicy;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public void setShardId(ShardId shardId) {
        this.shardId = shardId;
    }

    /** Whether the fragment contains a PARTIAL aggregate instruction. */
    public boolean hasPartialAggregate() {
        return hasPartialAggregate;
    }

    public void setHasPartialAggregate(boolean hasPartialAggregate) {
        this.hasPartialAggregate = hasPartialAggregate;
    }

    /** Returns the tracker used to attribute callback work to the owning task. */
    public DelegationThreadTracker getDelegationThreadTracker() {
        return delegationThreadTracker;
    }

    /** Sets the tracker used to attribute callback work to the owning task. */
    public void setDelegationThreadTracker(DelegationThreadTracker delegationThreadTracker) {
        this.delegationThreadTracker = delegationThreadTracker;
    }
}
