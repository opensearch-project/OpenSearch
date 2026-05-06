/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.spi.CommonExecutionContext;
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
    private MapperService mapperService;
    private IndexSettings indexSettings;

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
}
