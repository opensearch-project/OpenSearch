/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.datafusion.core.SessionContext;

import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

/**
 * Represents a stream of Apache Arrow record batches from DataFusion query execution.
 * Provides a Java interface to iterate through query results in a memory-efficient way.
 */
public class RecordBatchStream {

    private final SessionContext context;
    private final long streamPointer;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;
    private boolean initialized = false;
    private VectorSchemaRoot vectorSchemaRoot = null;

    /**
     * Creates a new RecordBatchStream for the given stream pointer
     * @param ctx the session context
     * @param streamId pointer to the native stream
     * @param allocator memory allocator for Arrow vectors
     */
    public RecordBatchStream(SessionContext ctx, long streamId, BufferAllocator allocator) {
        this.context = ctx;
        this.streamPointer = streamId;
        this.allocator = allocator;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    /**
     * Gets the Arrow VectorSchemaRoot for accessing the current batch data
     * @return the VectorSchemaRoot containing the current batch
     */
    public VectorSchemaRoot getVectorSchemaRoot() {
        ensureInitialized();
        return vectorSchemaRoot;
    }

    private Schema getSchema() {
        // Native method is not async, but use a future to store the result for convenience
        CompletableFuture<Schema> result = new CompletableFuture<>();
        getSchema(streamPointer, (errString, arrowSchemaAddress) -> {
            if (ErrorUtil.containsError(errString)) {
                result.completeExceptionally(new RuntimeException(errString));
            } else {
                try {
                    ArrowSchema arrowSchema = ArrowSchema.wrap(arrowSchemaAddress);
                    Schema schema = importSchema(allocator, arrowSchema, dictionaryProvider);
                    result.complete(schema);
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
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
        long runtimePointer = context.getRuntime();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        next(runtimePointer, streamPointer, (errString, arrowArrayAddress) -> {
            if (ErrorUtil.containsError(errString)) {
                result.completeExceptionally(new RuntimeException(errString));
            } else if (arrowArrayAddress == 0) {
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
        });
        return result;
    }

    /**
     * Closes the stream and releases all associated resources
     * @throws Exception if an error occurs during cleanup
     */
    public void close() throws Exception {
        closeStream(streamPointer);
        dictionaryProvider.close();
        if (initialized) {
            vectorSchemaRoot.close();
        }
    }

    private static native void next(long runtime, long pointer, ObjectResultCallback callback);

    private static native void getSchema(long pointer, ObjectResultCallback callback);

    private static native void closeStream(long pointer);
}
