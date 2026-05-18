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
 * Result of a delete operation. Sealed hierarchy with {@link Success} and {@link Failure} variants.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public sealed interface DeleteResult permits DeleteResult.Success, DeleteResult.Failure {

    /**
     * Successful delete result.
     *
     * @param version the version after to delete
     * @param primaryTerm the primary term
     * @param seqNo the sequence number
     */
    record Success(long version, long primaryTerm, long seqNo) implements DeleteResult {
    }

    /**
     * Failed delete result.
     *
     * @param cause the exception that caused the failure
     */
    record Failure(Exception cause) implements DeleteResult {
    }
}
