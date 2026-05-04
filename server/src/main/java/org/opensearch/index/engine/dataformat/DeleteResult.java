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
 * Result of a document delete operation. Sealed to ensure exhaustive handling
 * of success and failure cases.
 */
@ExperimentalApi
public sealed interface DeleteResult {

    /**
     * Indicates a successful delete.
     *
     * @param version the document version after deletion
     * @param term the primary term of the shard
     * @param seqNo the sequence number assigned to this delete operation
     */
    record Success(long version, long term, long seqNo) implements DeleteResult {
    }

    /**
     * Indicates a failed delete.
     *
     * @param cause the exception that caused the failure
     * @param version the document version at the time of failure
     * @param term the primary term of the shard
     * @param seqNo the sequence number assigned to this delete operation
     */
    record Failure(Exception cause, long version, long term, long seqNo) implements DeleteResult {
    }
}
