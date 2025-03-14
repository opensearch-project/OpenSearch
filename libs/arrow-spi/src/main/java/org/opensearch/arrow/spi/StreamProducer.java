/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.tasks.TaskId;

import java.io.Closeable;

/**
 * Represents a producer of Arrow streams. The producer first needs to define the job by implementing this interface and
 * then register the job with the {@link StreamManager#registerStream(StreamProducer, TaskId)}, which will return {@link StreamTicket}
 * which can be distributed to the consumer. The consumer can then use the ticket to retrieve the stream using
 * {@link StreamManager#getStreamReader(StreamTicket)} and then consume the stream using {@link StreamReader}.
 * <p>
 * BatchedJob supports streaming of intermediate results, allowing consumers to begin processing data before the entire
 * result set is generated. This is particularly useful for memory-intensive operations or when dealing with large datasets
 * that shouldn't be held entirely in memory.
 * <p>
 * Example usage:
 * <pre>{@code
 * public class QueryStreamProducer implements StreamProducer {
 *     private final SearchRequest searchRequest;
 *     private static final int BATCH_SIZE = 1000;
 *
 *     @Override
 *     public VectorSchemaRoot createRoot(BufferAllocator allocator) {
 *         List<Field> fields = Arrays.asList(
 *             Field.nullable("id", FieldType.valueOf(MinorType.VARCHAR)),
 *             Field.nullable("score", FieldType.valueOf(MinorType.FLOAT8))
 *         );
 *         return VectorSchemaRoot.create(new Schema(fields), allocator);
 *     }
 *
 *     @Override
 *     public BatchedJob createJob(BufferAllocator allocator) {
 *         return new BatchedJob() {
 *             @Override
 *             public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
 *                 SearchResponse response = client.search(searchRequest);
 *                 int currentBatch = 0;
 *
 *                 VarCharVector idVector = (VarCharVector) root.getVector("id");
 *                 Float8Vector scoreVector = (Float8Vector) root.getVector("score");
 *
 *                 for (SearchHit hit : response.getHits()) {
 *                     idVector.setSafe(currentBatch, hit.getId().getBytes());
 *                     scoreVector.setSafe(currentBatch, hit.getScore());
 *                     currentBatch++;
 *
 *                     if (currentBatch >= BATCH_SIZE) {
 *                         root.setRowCount(currentBatch);
 *                         flushSignal.awaitConsumption(1000);
 *                         currentBatch = 0;
 *                     }
 *                 }
 *             }
 *         };
 *     }
 * }
 *
 * // Usage:
 * StreamProducer producer = new QueryStreamProducer(searchRequest);
 * StreamTicket ticket = streamManager.registerStream(producer, taskId);
 * }</pre>
 *
 * @see StreamManager
 * @see StreamTicket
 * @see StreamReader
 */
@ExperimentalApi
public interface StreamProducer extends Closeable {

    /**
     * Creates a VectorSchemaRoot that defines the schema for this stream. This schema will be used
     * for all batches produced by this stream.
     *
     * @param allocator The allocator to use for creating vectors
     * @return A new VectorSchemaRoot instance
     */
    VectorSchemaRoot createRoot(BufferAllocator allocator);

    /**
     * Creates a job that will produce the stream data in batches. The job will populate
     * the VectorSchemaRoot and use FlushSignal to coordinate with consumers.
     *
     * @param allocator The allocator to use for any additional memory allocations
     * @return A new BatchedJob instance
     */
    BatchedJob createJob(BufferAllocator allocator);

    /**
     * Provides an estimate of the total number of rows that will be produced.
     *
     * @return Estimated number of rows, or -1 if unknown
     */
    int estimatedRowCount();

    /**
     * Task action name
     * @return action name
     */
    String getAction();

    /**
     * BatchedJob interface for producing stream data in batches.
     */
    interface BatchedJob {

        /**
         * Executes the batch processing job. Implementations should populate the root with data
         * and use flushSignal to coordinate with consumers when each batch is ready.
         *
         * @param root The VectorSchemaRoot to populate with data
         * @param flushSignal Signal to coordinate with consumers
         */
        void run(VectorSchemaRoot root, FlushSignal flushSignal);

        /**
         * Called to signal producer when the job is canceled.
         * This method is used to clean up resources or cancel ongoing operations.
         * This maybe called from a different thread than the one used for run(). It might be possible that run()
         * thread is busy when onCancel() is called and wakes up later. In such cases, ensure that run() terminates early
         * and should clean up resources.
         */
        void onCancel();

        /**
         * Producers can set isCancelled flag to true to indicate that the job is canceled.
         * This will ensure the stream is closed and no more data is produced from next Batch onwards.
         *
         * @return true if the job is canceled, false otherwise
         */
        boolean isCancelled();
    }

    /**
     * Functional interface for managing stream consumption signals.
     */
    @FunctionalInterface
    interface FlushSignal {
        /**
         * Blocks until the current batch has been consumed or timeout occurs.
         *
         * @param timeout Maximum milliseconds to wait
         */
        void awaitConsumption(int timeout);
    }
}
