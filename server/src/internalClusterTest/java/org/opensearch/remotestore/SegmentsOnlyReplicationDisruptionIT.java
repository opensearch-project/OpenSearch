/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.junit.Before;

import java.nio.file.Path;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

/**
 * Runs the remote store segment replication disruption suite against nodes in {@code segments_only} mode. Segment
 * replication reads from the segment repository there exactly as it does in a fully remote backed cluster, so
 * cancellation and checkpoint propagation have to behave the same way when the translog stays on local disk.
 */
public class SegmentsOnlyReplicationDisruptionIT extends SegmentReplicationUsingRemoteStoreDisruptionIT {

    @Before
    public void startNodesInSegmentsOnlyMode() {
        segmentsOnly = true;
    }

    /** Every inherited test starts its nodes through here, so this is where the mode can be held to. */
    @Override
    public Settings buildRemoteStoreNodeAttributes(Path repoLocation, double ioFailureRate, String skipExceptionBlobList, long maxFailure) {
        Settings attributes = super.buildRemoteStoreNodeAttributes(repoLocation, ioFailureRate, skipExceptionBlobList, maxFailure);
        assertNull(
            "a segments_only node must have no translog repository",
            attributes.get("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY)
        );
        return attributes;
    }
}
