/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class QueryGroupStateTests extends OpenSearchTestCase {
    QueryGroupState queryGroupState;

    public void testRandomQueryGroupsStateUpdates() {
        queryGroupState = new QueryGroupState();
        List<Thread> updaterThreads = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            if (i % 5 == 0) {
                updaterThreads.add(new Thread(() -> queryGroupState.incrementCompletions()));
            } else if (i % 5 == 1) {
                updaterThreads.add(new Thread(() -> queryGroupState.incrementRejections()));
            } else if (i % 5 == 2) {
                updaterThreads.add(new Thread(() -> queryGroupState.incrementFailures()));
            } else if (i % 5 == 3) {
                updaterThreads.add(new Thread(() -> queryGroupState.getResourceState().get(ResourceType.CPU).incrementCancellations()));
            } else {
                updaterThreads.add(new Thread(() -> queryGroupState.getResourceState().get(ResourceType.MEMORY).incrementCancellations()));
            }

            if (i%5 == 3 || i%5 == 4) {
                updaterThreads.add(new Thread(() -> queryGroupState.incrementTotalCancellations()));
            }
        }

        // trigger the updates
        updaterThreads.forEach(Thread::start);
        // wait for updates to be finished
        updaterThreads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException ignored) {

            }
        });

        assertEquals(5, queryGroupState.getCompletions());
        assertEquals(5, queryGroupState.getRejections());
        assertEquals(5, queryGroupState.getFailures());
        assertEquals(10, queryGroupState.getTotalCancellations());
        assertEquals(5, queryGroupState.getResourceState().get(ResourceType.CPU).getCancellations());
        assertEquals(5, queryGroupState.getResourceState().get(ResourceType.MEMORY).getCancellations());
    }

}
