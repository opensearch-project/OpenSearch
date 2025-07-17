/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Defines the strategy for resetting a pointer's position within an ingestion shard.
 * This enum specifies different ways to determine where the pointer should be positioned
 * when a reset operation is performed.
 */
@ExperimentalApi
public enum ResetState {
    EARLIEST,
    LATEST,
    RESET_BY_OFFSET,
    RESET_BY_TIMESTAMP,
    NONE,
}
