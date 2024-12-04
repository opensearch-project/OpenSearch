/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * StreamReader is a stateful iterator that can be used to read data from a stream.
 * It is used to read data from a stream in batches. The iterator will return a
 * VectorSchemaRoot that contains the data for the current batch. The iterator will
 * return true if there is more data to read, false if the stream is exhausted.
 * Example usage:
 * <pre>{@code
 * // producer
 * StreamProducer producer = new QueryStreamProducer(searchRequest);
 * StreamTicket ticket = streamManager.registerStream(producer, taskId);
 *
 * // consumer
 * StreamReader iterator = streamManager.getStreamReader(ticket);
 * try (VectorSchemaRoot root = iterator.getRoot()) {
 *     while (iterator.next()) {
 *         VarCharVector idVector = (VarCharVector)root.getVector("id");
 *         Float8Vector scoreVector = (Float8Vector) root.getVector("score");
 *     }
 * }
 * }</pre>
 *
 * @see StreamProducer
 */
@ExperimentalApi
public interface StreamReader extends Closeable {

    /**
     * Blocking request to load next batch into root.
     *
     * @return true if more data was found, false if the stream is exhausted
     */
    boolean next();

    /**
     * Returns the VectorSchemaRoot associated with this iterator.
     * The content of this root is updated with each successful call to next().
     *
     * @return the VectorSchemaRoot
     */
    VectorSchemaRoot getRoot();
}
