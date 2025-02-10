/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.clustermanager;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class ClusterManagerTaskThrottlingIT extends OpenSearchIntegTestCase {

    /*
     * This integ test will test end-end cluster manager throttling feature for
     * remote cluster manager.
     *
     * It will check the number of request coming to cluster manager node
     * should be total number of requests + throttled requests from cluster manager.
     * This will ensure the end-end feature is working as cluster manager is throwing
     * Throttling exception and data node is performing retries on it.
     *
     */
    public void testThrottlingForRemoteClusterManager() throws Exception {
        try {
            internalCluster().beforeTest(random());
            String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
            String dataNode = internalCluster().startDataOnlyNode();
            int throttlingLimit = randomIntBetween(1, 5);
            createIndex("test");
            setPutMappingThrottlingLimit(throttlingLimit);

            TransportService clusterManagerTransportService = (internalCluster().getInstance(TransportService.class, clusterManagerNode));
            AtomicInteger requestCountOnClusterManager = new AtomicInteger();
            AtomicInteger throttledRequest = new AtomicInteger();
            int totalRequest = randomIntBetween(throttlingLimit, 5 * throttlingLimit);
            CountDownLatch latch = new CountDownLatch(totalRequest);

            clusterManagerTransportService.addMessageListener(new TransportMessageListener() {
                @Override
                public void onRequestReceived(long requestId, String action) {
                    if (action.contains("mapping")) {
                        requestCountOnClusterManager.incrementAndGet();
                    }
                }

                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    if (action.contains("mapping")) {
                        throttledRequest.incrementAndGet();
                        assertEquals(ClusterManagerThrottlingException.class, error.getClass());
                    }
                }
            });

            ActionListener listener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    throw new AssertionError(e);
                }
            };

            executePutMappingRequests(totalRequest, dataNode, listener);
            latch.await();

            assertEquals(totalRequest + throttledRequest.get(), requestCountOnClusterManager.get());
            assertBusy(
                () -> { assertEquals(clusterService().getClusterManagerService().numberOfThrottledPendingTasks(), throttledRequest.get()); }
            );
        } finally {
            clusterSettingCleanUp();
        }
    }

    /*
     * This will test the throttling feature for single node.
     *
     * Here we will assert the client behaviour that client's request is not
     * failed, i.e. Throttling exception is not passed to the client.
     * Data node will internally do the retry and request should pass.
     *
     */
    public void testThrottlingForSingleNode() throws Exception {
        try {
            internalCluster().beforeTest(random());
            String node = internalCluster().startNode();
            int throttlingLimit = randomIntBetween(1, 5);
            createIndex("test");
            setPutMappingThrottlingLimit(throttlingLimit);

            AtomicInteger successfulRequest = new AtomicInteger();
            int totalRequest = randomIntBetween(throttlingLimit, 3 * throttlingLimit);
            CountDownLatch latch = new CountDownLatch(totalRequest);

            ActionListener listener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    successfulRequest.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    throw new AssertionError(e);
                }
            };
            executePutMappingRequests(totalRequest, node, listener);

            latch.await();
            assertEquals(totalRequest, successfulRequest.get());
        } finally {
            clusterSettingCleanUp();
        }
    }

    /*
     * This will test the timeout of tasks during throttling.
     *
     * Here we will assert the client behaviour that client's request is not
     * failed with throttling exception but timeout exception.
     * It also verifies that if limit is set to 0, all tasks are getting timedout.
     */

    public void testTimeoutWhileThrottling() throws Exception {
        try {
            internalCluster().beforeTest(random());
            String node = internalCluster().startNode();
            int throttlingLimit = 0; // throttle all the tasks
            createIndex("test");
            setPutMappingThrottlingLimit(throttlingLimit);

            AtomicInteger timedoutRequest = new AtomicInteger();
            int totalRequest = randomIntBetween(1, 5);
            CountDownLatch latch = new CountDownLatch(totalRequest);

            ActionListener listener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    latch.countDown();
                    throw new AssertionError("Request should not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    timedoutRequest.incrementAndGet();
                    latch.countDown();
                    assertTrue(e instanceof ProcessClusterEventTimeoutException);
                }
            };
            executePutMappingRequests(totalRequest, node, listener);

            latch.await();
            assertEquals(totalRequest, timedoutRequest.get()); // verifying all requests were timed out with 0 throttling limit
        } finally {
            clusterSettingCleanUp();
        }
    }

    private void executePutMappingRequests(int totalRequest, String node, ActionListener listener) throws Exception {
        Thread[] threads = new Thread[totalRequest];
        for (int i = 0; i < totalRequest; i++) {
            PutMappingRequest putMappingRequest = new PutMappingRequest("test").source("field" + i, "type=text");
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    internalCluster().client(node).admin().indices().putMapping(putMappingRequest, listener);
                }
            });
        }
        for (int i = 0; i < totalRequest; i++) {
            threads[i].run();
        }
        for (int i = 0; i < totalRequest; i++) {
            threads[i].join();
        }
    }

    private void setPutMappingThrottlingLimit(int throttlingLimit) {
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        Settings settings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", throttlingLimit).build();
        settingsRequest.transientSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());
    }

    private void clusterSettingCleanUp() {
        // We need to remove the throttling limit from setting as part of test cleanup
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        Settings settings = Settings.builder().put("cluster_manager.throttling.thresholds.put-mapping.value", (String) null).build();
        settingsRequest.transientSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());
    }
}
