/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Set;
import org.opensearch.common.collect.Tuple;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStorePinnedTimestampsIT extends RemoteStoreBaseIntegTestCase {
    static final String INDEX_NAME = "remote-store-test-idx-1";

    ActionListener<Void> noOpActionListener = new ActionListener<>() {
        @Override
        public void onResponse(Void unused) {}

        @Override
        public void onFailure(Exception e) {}
    };

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    public void testTimestampPinUnpin() throws Exception {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(RemoteStorePinnedTimestampService.class, primaryNodeName(INDEX_NAME));

        Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp = RemoteStorePinnedTimestampService.getPinnedTimestamps();
        long lastFetchTimestamp = pinnedTimestampWithFetchTimestamp.v1();
        assertEquals(-1L, lastFetchTimestamp);
        assertEquals(Set.of(), pinnedTimestampWithFetchTimestamp.v2());

        assertThrows(IllegalArgumentException.class, () -> remoteStorePinnedTimestampService.pinTimestamp(1234L, "ss1", noOpActionListener));

        long timestamp1 = System.currentTimeMillis() + 30000L;
        long timestamp2 = System.currentTimeMillis() + 60000L;
        long timestamp3 = System.currentTimeMillis() + 900000L;
        remoteStorePinnedTimestampService.pinTimestamp(timestamp1, "ss2", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp2, "ss3", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp3, "ss4", noOpActionListener);

        remoteStorePinnedTimestampService.setPinnedTimestampsSchedulerInterval(TimeValue.timeValueSeconds(1));

        assertBusy(() -> {
            Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp_2 = RemoteStorePinnedTimestampService.getPinnedTimestamps();
            long lastFetchTimestamp_2 = pinnedTimestampWithFetchTimestamp_2.v1();
            assertTrue(lastFetchTimestamp_2 != -1);
            assertEquals(Set.of(timestamp1, timestamp2, timestamp3), pinnedTimestampWithFetchTimestamp_2.v2());
        });

        remoteStorePinnedTimestampService.setPinnedTimestampsSchedulerInterval(TimeValue.timeValueMinutes(3));

        // This should be a no-op as pinning entity is different
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp1, "no-snapshot", noOpActionListener);
        // Unpinning already pinned entity
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp2, "ss3", noOpActionListener);
        // Adding different entity to already pinned timestamp
        remoteStorePinnedTimestampService.pinTimestamp(timestamp3, "ss5", noOpActionListener);

        remoteStorePinnedTimestampService.setPinnedTimestampsSchedulerInterval(TimeValue.timeValueSeconds(1));

        assertBusy(() -> {
            Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp_3 = RemoteStorePinnedTimestampService.getPinnedTimestamps();
            long lastFetchTimestamp_3 = pinnedTimestampWithFetchTimestamp_3.v1();
            assertTrue(lastFetchTimestamp_3 != -1);
            assertEquals(Set.of(timestamp1, timestamp3), pinnedTimestampWithFetchTimestamp_3.v2());
        });

        remoteStorePinnedTimestampService.setPinnedTimestampsSchedulerInterval(TimeValue.timeValueMinutes(3));
    }
}
