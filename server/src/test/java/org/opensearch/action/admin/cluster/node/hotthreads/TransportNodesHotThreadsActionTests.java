/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.hotthreads;

import org.opensearch.monitor.jvm.HotThreads;
import org.opensearch.test.OpenSearchTestCase;

public class TransportNodesHotThreadsActionTests extends OpenSearchTestCase {

    private static final int MAX_SNAPSHOTS = 500;

    public void testValidSnapshotsPasses() {
        // default value as well as the accepted boundaries must not be rejected
        TransportNodesHotThreadsAction.validateRequestParams(new NodesHotThreadsRequest(), MAX_SNAPSHOTS);
        TransportNodesHotThreadsAction.validateRequestParams(new NodesHotThreadsRequest().snapshots(1), MAX_SNAPSHOTS);
        TransportNodesHotThreadsAction.validateRequestParams(new NodesHotThreadsRequest().snapshots(MAX_SNAPSHOTS), MAX_SNAPSHOTS);
    }

    public void testSnapshotsAboveMaxRejected() {
        // a very large value would otherwise allocate an oversized ThreadInfo[snapshots][] array
        NodesHotThreadsRequest request = new NodesHotThreadsRequest().snapshots(500000000);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportNodesHotThreadsAction.validateRequestParams(request, MAX_SNAPSHOTS)
        );
        assertTrue(e.getMessage().contains("[snapshots]"));
        assertTrue(e.getMessage().contains(HotThreads.MAX_HOT_THREADS_SNAPSHOTS_SETTING.getKey()));
    }

    public void testSnapshotsJustAboveMaxRejected() {
        NodesHotThreadsRequest request = new NodesHotThreadsRequest().snapshots(MAX_SNAPSHOTS + 1);
        expectThrows(
            IllegalArgumentException.class,
            () -> TransportNodesHotThreadsAction.validateRequestParams(request, MAX_SNAPSHOTS)
        );
    }

    public void testSnapshotsBelowOneRejected() {
        for (int invalid : new int[] { 0, -1, Integer.MIN_VALUE }) {
            NodesHotThreadsRequest request = new NodesHotThreadsRequest().snapshots(invalid);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> TransportNodesHotThreadsAction.validateRequestParams(request, MAX_SNAPSHOTS)
            );
            assertTrue(e.getMessage().contains("[snapshots]"));
        }
    }
}
