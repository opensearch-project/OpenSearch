/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.datafusion.jni.handle.StreamHandle;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a stream of Apache Arrow record batches from DataFusion query execution.
 * Provides a Java interface to iterate through query results in a memory-efficient way.
 */
public class RecordBatchStream {

    private final StreamHandle streamHandle;
    private final RootAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;
    private final CompletableFuture<VectorSchemaRoot> schemaFuture;
    private volatile VectorSchemaRoot vectorSchemaRoot;

    /**
     * Creates a new RecordBatchStream for the given stream pointer
     * @param streamId the stream pointer
     * @param runtimePtr the runtime pointer
     */
    public RecordBatchStream(long streamId, long runtimePtr) {
        this.streamHandle = new StreamHandle(streamId, runtimePtr);
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.dictionaryProvider = new CDataDictionaryProvider();
        this.schemaFuture = streamHandle.getSchema(allocator, dictionaryProvider)
            .thenApply(schema -> VectorSchemaRoot.create(schema, allocator));
    }

    /**
     * Waits for schema initialization to complete
     * @return true when initialized
     */
    public boolean isInitialized() {
        if (vectorSchemaRoot == null) {
            vectorSchemaRoot = schemaFuture.join();
        }
        return true;
    }

    /**
     * Gets the Arrow VectorSchemaRoot for accessing the current batch data
     * @return the VectorSchemaRoot containing the current batch
     */
    public VectorSchemaRoot getVectorSchemaRoot() {
        isInitialized();
        return vectorSchemaRoot;
    }

    /**
     * Loads the next batch of data from the stream
     * @return a CompletableFuture that completes with true if more data is available, false if end of stream
     */
    public CompletableFuture<Boolean> loadNextBatch() {
        isInitialized();
        return streamHandle.loadNextBatch(allocator, vectorSchemaRoot, dictionaryProvider);
    }

    /**
     * Closes the stream and releases all associated resources
     * @throws Exception if an error occurs during cleanup
     */
    public void close() throws Exception {
        streamHandle.close();
        dictionaryProvider.close();
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
        }
        allocator.close();
    }
}
