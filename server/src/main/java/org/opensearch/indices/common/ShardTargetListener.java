/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.common;

import org.opensearch.OpenSearchException;

/**
 * Interface for listeners that run when there's a change in replication state
 *
 * @opensearch.internal
 */
public interface ShardTargetListener {

    void onDone(ShardTargetState state);

    void onFailure(ShardTargetState state, OpenSearchException e, boolean sendShardFailure);
}
