/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ResourceType;

import java.util.ArrayList;
import java.util.List;

public class QueryGroupStateTests extends OpenSearchTestCase {
    QueryGroupState queryGroupState;

    public void testRandomQueryGroupsStateUpdates() {
        queryGroupState = new QueryGroupState();
        List<Thread> updaterThreads = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            if (i % 5 == 0) {
                updaterThreads.add(new Thread(() -> queryGroupState.completions.inc()));
            } else if (i % 5 == 1) {
                updaterThreads.add(new Thread(() -> {
                    queryGroupState.totalRejections.inc();
                    if (randomBoolean()) {
                        queryGroupState.getResourceState().get(ResourceType.CPU).rejections.inc();
                    } else {
                        queryGroupState.getResourceState().get(ResourceType.MEMORY).rejections.inc();
                    }
                }));
            } else if (i % 5 == 2) {
                updaterThreads.add(new Thread(() -> queryGroupState.failures.inc()));
            } else if (i % 5 == 3) {
                updaterThreads.add(new Thread(() -> queryGroupState.getResourceState().get(ResourceType.CPU).cancellations.inc()));
            } else {
                updaterThreads.add(new Thread(() -> queryGroupState.getResourceState().get(ResourceType.MEMORY).cancellations.inc()));
            }

            if (i % 5 == 3 || i % 5 == 4) {
                updaterThreads.add(new Thread(() -> queryGroupState.totalCancellations.inc()));
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
        assertEquals(5, queryGroupState.getTotalRejections());

        final long sumOfRejectionsDueToResourceTypes = queryGroupState.getResourceState().get(ResourceType.CPU).rejections.count()
            + queryGroupState.getResourceState().get(ResourceType.MEMORY).rejections.count();
        assertEquals(sumOfRejectionsDueToResourceTypes, queryGroupState.getTotalRejections());

        assertEquals(5, queryGroupState.getFailures());
        assertEquals(10, queryGroupState.getTotalCancellations());
        assertEquals(5, queryGroupState.getResourceState().get(ResourceType.CPU).cancellations.count());
        assertEquals(5, queryGroupState.getResourceState().get(ResourceType.MEMORY).cancellations.count());
    }

}
