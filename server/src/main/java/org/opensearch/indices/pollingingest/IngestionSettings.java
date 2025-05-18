/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Holds the ingestion settings required to update the poller. All values are optional to support partial update.
 * @param isPaused Indicates if poller needs to be paused or resumed.
 * @param resetState Optional. Indicates target reset state of the poller.
 * @param resetValue Optional. Indicates target reset value (offset/timestamp/sequence number etc).
 */
@ExperimentalApi
public record IngestionSettings(@Nullable Boolean isPaused, @Nullable StreamPoller.ResetState resetState, @Nullable String resetValue) {
}
