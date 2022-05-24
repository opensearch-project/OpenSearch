/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTimer;

/**
 * ReplicationState implementation to track Segment Replication events.
 *
 * @opensearch.internal
 */
public class SegmentReplicationState implements ReplicationState {

    @Override
    public ReplicationLuceneIndex getIndex() {
        return null;
    }

    @Override
    public ReplicationTimer getTimer() {
        return null;
    }
}
