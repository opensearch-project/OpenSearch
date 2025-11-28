/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.datafusion.jni.NativeBridge;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.StreamHandle;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a stream of Apache Arrow record batches from DataFusion query execution.
 * Provides a Java interface to iterate through query results in a memory-efficient way.
 */
public class RecordBatchStream implements Closeable {

    private final StreamHandle streamHandle;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;
    private final CompletableFuture<VectorSchemaRoot> schemaFuture;
    private volatile VectorSchemaRoot vectorSchemaRoot;

    /**
     * Creates a new RecordBatchStream for the given stream pointer
     * @param streamId the stream pointer
     * @param runtimePtr the runtime pointer
     * @param parentAllocator parent allocator to create child from
     */
    public RecordBatchStream(long streamId, long runtimePtr, BufferAllocator parentAllocator) {
        this.streamHandle = new StreamHandle(streamId, runtimePtr);
        this.allocator = parentAllocator.newChildAllocator("stream-" + streamId, 0, Long.MAX_VALUE);
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

    private Schema getSchema() {
        // Native method is not async, but use a future to store the result for convenience
        CompletableFuture<Schema> result = new CompletableFuture<>();
        getSchema(streamPointer, new ActionListener<Long>() {
            @Override
            public void onResponse(Long arrowSchemaAddress) {
                try {
                    ArrowSchema arrowSchema = ArrowSchema.wrap(arrowSchemaAddress);
                    Schema schema = importSchema(allocator, arrowSchema, dictionaryProvider);
                    result.complete(schema);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result.join();
    }

    private Schema importSchema(BufferAllocator allocator, ArrowSchema schema, CDataDictionaryProvider provider) {
        Field structField = importField(allocator, schema, provider);
        if (structField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
            throw new IllegalArgumentException("Cannot import schema: ArrowSchema describes non-struct type");
        }
        return new Schema(structField.getChildren(), structField.getMetadata());
    }

    private void ensureInitialized() {
        if (!initialized) {
            Schema schema = getSchema();
            this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        }
        initialized = true;
    }

    /**
     * Loads the next batch of data from the stream
     * @return a CompletableFuture that completes with true if more data is available, false if end of stream
     */
    public CompletableFuture<Boolean> loadNextBatch() {
        ensureInitialized();
        long runtimePointer = this.runtimePtr;
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        next(runtimePointer, streamPointer, new ActionListener<Long>() {
            @Override
            public void onResponse(Long arrowArrayAddress) {
                if (arrowArrayAddress == 0) {
                    // Reached end of stream
                    result.complete(false);
                } else {
                    try {
                        ArrowArray arrowArray = ArrowArray.wrap(arrowArrayAddress);
                        Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, dictionaryProvider);
                        result.complete(true);
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /**
     * Closes the stream and releases all associated resources
     * @throws IOException if an error occurs during cleanup
     */
    @Override
    public void close() throws IOException {
        streamHandle.close();
        dictionaryProvider.close();
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
        }
        allocator.close();
    }
}
