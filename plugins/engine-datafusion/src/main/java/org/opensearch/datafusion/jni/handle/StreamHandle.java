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
     * @param provider dictionary provider
     * @return CompletableFuture with the schema
     */
    public CompletableFuture<Schema> getSchema(BufferAllocator allocator, CDataDictionaryProvider provider) {
        CompletableFuture<Schema> result = new CompletableFuture<>();
        NativeBridge.streamGetSchema(getPointer(), (errString, arrowSchemaAddress) -> {
            if (ErrorUtil.containsError(errString)) {
                result.completeExceptionally(new RuntimeException(errString));
            } else {
                try {
                    ArrowSchema arrowSchema = ArrowSchema.wrap(arrowSchemaAddress);
                    Schema schema = importSchema(allocator, arrowSchema, provider);
                    result.complete(schema);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    /**
     * Loads the next batch into the provided VectorSchemaRoot.
     * @param allocator memory allocator
     * @param root the VectorSchemaRoot to load data into
     * @param provider dictionary provider
     * @return CompletableFuture with true if more data available, false if end of stream
     */
    public CompletableFuture<Boolean> loadNextBatch(
        BufferAllocator allocator,
        VectorSchemaRoot root,
        CDataDictionaryProvider provider
    ) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        NativeBridge.streamNext(runtimePtr, getPointer(), (errString, arrowArrayAddress) -> {
            if (ErrorUtil.containsError(errString)) {
                result.completeExceptionally(new RuntimeException(errString));
            } else if (arrowArrayAddress == 0) {
                result.complete(false);
            } else {
                try {
                    ArrowArray arrowArray = ArrowArray.wrap(arrowArrayAddress);
                    Data.importIntoVectorSchemaRoot(allocator, arrowArray, root, provider);
                    result.complete(true);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
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
