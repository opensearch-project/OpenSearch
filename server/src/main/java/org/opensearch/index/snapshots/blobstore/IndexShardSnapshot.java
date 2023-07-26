/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.snapshots.blobstore;

import org.opensearch.index.snapshots.IndexShardSnapshotStatus;

/**
 * Base interface for shard snapshot status
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface IndexShardSnapshot {
    IndexShardSnapshotStatus getIndexShardSnapshotStatus();
}
