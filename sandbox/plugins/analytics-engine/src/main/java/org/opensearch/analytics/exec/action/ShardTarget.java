/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.shard.ShardId;

/**
 * Resolved shard + node pairing for dispatch. Analogous to
 * {@code SearchShardIterator} in vanilla OpenSearch search.
 *
 * @opensearch.internal
 */
public record ShardTarget(ShardId shardId, DiscoveryNode node) {
}
