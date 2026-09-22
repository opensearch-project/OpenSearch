/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

/**
 * Represents the lifecycle states of a VectorSchemaRoot.
 */
public enum VSRState {
    /** Currently accepting writes. */
    ACTIVE,
    /** Read-only, queued for flush. */
    FROZEN,
    /** Completed and cleaned up. */
    CLOSED
}
