/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.index.shard.ShardId;

/**
 * Cross-cutting shape for any shard-targeted analytics action: identifies the originating
 * query, the stage, and the target shard. Implemented by {@link FragmentExecutionRequest}
 * and {@link FetchByRowIdsRequest}; lets shared infrastructure (failure-listener wrapping,
 * resource accounting, logging) operate on either request type without an instanceof check.
 *
 * @opensearch.internal
 */
public interface ShardInvocationRequest {
    String getQueryId();

    int getStageId();

    ShardId getShardId();
}
