/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

/**
 * Exception thrown when a tiering operation is rejected due to validation failures.
 * This exception wraps the original exception and provides structured rejection reasons
 * for better metrics tracking and error handling.
 *
 * @opensearch.internal
 */
public class TieringRejectionException extends RuntimeException {

    /**
     * Enumeration of all possible tiering rejection reasons for structured error handling.
     */
    public enum RejectionReason {
        /** Rejection: no warm nodes available. */
        NO_WARM_NODES,
        /** Rejection: index in RED health status. */
        INDEX_RED_STATUS,
        /** Rejection: high JVM utilization. */
        HIGH_JVM_UTILIZATION,
        /** Rejection: insufficient space on warm tier. */
        INSUFFICIENT_SPACE_WARM,
        /** Rejection: insufficient space on hot tier. */
        INSUFFICIENT_SPACE_HOT,
        /** Rejection: CCR follower index cannot be tiered. */
        CCR_INDEX_REJECTION,
        /** Rejection: remote store is not enabled. */
        REMOTE_STORE_NOT_ENABLED,
        /** Rejection: invalid tier transition requested. */
        INVALID_TIER_TRANSITION,
        /** Rejection: largest shard exceeds available space. */
        LARGEST_SHARD_SPACE_INSUFFICIENT,
        /** Rejection: high file cache utilization. */
        HIGH_FILE_CACHE_UTILIZATION,
        /** Rejection: concurrent tiering limit exceeded. */
        CONCURRENT_LIMIT_EXCEEDED,
        /** Rejection: shard limit exceeded. */
        SHARD_LIMIT_EXCEEDED
    }

    private final RejectionReason rejectionReason;

    /**
     * Constructs a TieringRejectionException with rejection reason and original exception.
     *
     * @param rejectionReason the structured reason for rejection
     * @param originalException the original exception that was thrown
     */
    public TieringRejectionException(RejectionReason rejectionReason, RuntimeException originalException) {
        super(originalException.getMessage(), originalException);
        this.rejectionReason = rejectionReason;
    }

    /**
     * Gets the structured rejection reason.
     *
     * @return the rejection reason enum value
    */
    public RejectionReason getRejectionReason() {
        return rejectionReason;
    }
}
