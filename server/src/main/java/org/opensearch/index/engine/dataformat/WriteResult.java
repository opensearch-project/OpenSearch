/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Sealed result of a document write operation, representing either a {@link Success}
 * or a {@link Failure}. Each variant carries the version, primary term, and sequence
 * number assigned (or attempted) by the engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public sealed interface WriteResult {

    /**
     * A successful write result.
     *
     * @param version  the document version assigned by the engine
     * @param term     the primary term under which the write was executed
     * @param seqNo    the sequence number assigned to the operation
     */
    record Success(long version, long term, long seqNo) implements WriteResult {
    }

    /**
     * A failed write result.
     *
     * @param cause    the exception that caused the write to fail
     * @param version  the document version at the time of failure, or {@code -1} if unavailable
     * @param term     the primary term at the time of failure, or {@code -1} if unavailable
     * @param seqNo    the sequence number at the time of failure, or {@code -1} if unassigned
     */
    record Failure(Exception cause, long version, long term, long seqNo) implements WriteResult {
    }
}
