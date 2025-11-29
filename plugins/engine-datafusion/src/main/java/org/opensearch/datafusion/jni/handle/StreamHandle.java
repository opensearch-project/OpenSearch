/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.ErrorUtil;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.jni.NativeHandle;

import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

/**
 * Type-safe handle for native DataFusion stream with Arrow integration.
 */
public final class StreamHandle extends NativeHandle {

    private final long runtimePtr;

    public StreamHandle(long ptr, long runtimePtr) {
        super(ptr);
        this.runtimePtr = runtimePtr;
    }

    @Override
    protected void doClose() {
        NativeBridge.streamClose(ptr);
    }

    /**
     * Gets the Arrow schema for this stream.
     * @param allocator memory allocator for Arrow
     * @param dictionaryProvider dictionary provider
     * @return CompletableFuture with the schema
     */
    public CompletableFuture<Schema> getSchema(BufferAllocator allocator, CDataDictionaryProvider dictionaryProvider) {
        // Native method is not async, but use a future to store the result for convenience
        CompletableFuture<Schema> result = new CompletableFuture<>();
        NativeBridge.streamGetSchema(ptr, new ActionListener<Long>() {
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
        return result;
    }

    /**
     * Loads the next batch of data from the stream
     * @return a CompletableFuture that completes with true if more data is available, false if end of stream
     */
    public CompletableFuture<Boolean> loadNextBatch(BufferAllocator allocator, VectorSchemaRoot vectorSchemaRoot,
                                                    CDataDictionaryProvider dictionaryProvider) {
        long runtimePointer = this.runtimePtr;
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        NativeBridge.streamNext(runtimePointer, ptr, new ActionListener<Long>() {
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

    private Schema importSchema(BufferAllocator allocator, ArrowSchema schema, CDataDictionaryProvider provider) {
        Field structField = importField(allocator, schema, provider);
        if (structField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
            throw new IllegalArgumentException("Cannot import schema: ArrowSchema describes non-struct type");
        }
        return new Schema(structField.getChildren(), structField.getMetadata());
    }
}
