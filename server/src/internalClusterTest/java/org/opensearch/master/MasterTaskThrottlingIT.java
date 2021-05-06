/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.master;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.master.MasterThrottlingRetryListener;
import org.opensearch.cluster.service.MasterTaskThrottlingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.TransportMessageListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class MasterTaskThrottlingIT extends OpenSearchIntegTestCase {

    private static final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);

    /*
     * This integ test will test end-end master throttling feature for
     * remote master.
     *
     * It will check the number of request coming to master node
     * should be total number of requests + throttled requests from master.
     * This will ensure the end-end feature is working as master is throwing
     * Throttling exception and data node is performing retries on it.
     *
     */
    public void testThrottlingForRemoteMaster() throws Exception {
        try {
            internalCluster().beforeTest(random(), 0);
            String masterNode = internalCluster().startMasterOnlyNode();
            String dataNode = internalCluster().startDataOnlyNode();
            int throttlingLimit = randomIntBetween(1, 5);
            createIndex("test");

            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder()
                    .put("master.throttling.thresholds.put-mapping.value", throttlingLimit)
                    .build();
            settingsRequest.transientSettings(settings);
            assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());

            TransportService masterTransportService = (internalCluster().getInstance(TransportService.class, masterNode));
            AtomicInteger requestCountOnMaster = new AtomicInteger();
            AtomicInteger throttledRequest = new AtomicInteger();
            int totalRequest = randomIntBetween(throttlingLimit, 5 * throttlingLimit);
            CountDownLatch latch = new CountDownLatch(totalRequest);

            masterTransportService.addMessageListener(new TransportMessageListener() {
                @Override
                public void onRequestReceived(long requestId, String action) {
                    if (action.contains("mapping")) {
                        requestCountOnMaster.incrementAndGet();
                    }
                }

                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    if (action.contains("mapping")) {
                        throttledRequest.incrementAndGet();
                        assertEquals(MasterTaskThrottlingException.class, error.getClass());
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

            Thread[] threads = new Thread[totalRequest];
            for (int i = 0; i < totalRequest; i++) {
                PutMappingRequest putMappingRequest = new PutMappingRequest("test")
                        .type("type")
                        .source("field" + i, "type=text");
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        internalCluster().client(dataNode).admin().indices().putMapping(putMappingRequest, listener);
                    }
                });
            }
            for (int i = 0; i < totalRequest; i++) {
                threads[i].run();
            }
            for (int i = 0; i < totalRequest; i++) {
                threads[i].join();
            }
            latch.await();

            assertEquals(totalRequest + throttledRequest.get(), requestCountOnMaster.get());
            assertBusy(() -> {
                assertEquals(clusterService().getMasterService().numberOfThrottledPendingTasks(), throttledRequest.get());
            });
            assertEquals(MasterThrottlingRetryListener.getRetryingTasksCount(), 0);
        }
        finally {
            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder()
                    .put("master.throttling.thresholds.put-mapping.value", (String) null)
                    .build();
            settingsRequest.transientSettings(settings);
            assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());
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
            internalCluster().beforeTest(random(), 0);
            String node = internalCluster().startNode();
            int throttlingLimit = randomIntBetween(1, 5);
            createIndex("test");

            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder()
                    .put("master.throttling.thresholds.put-mapping.value", throttlingLimit)
                    .build();
            settingsRequest.transientSettings(settings);
            assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());

            AtomicInteger successfulRequest = new AtomicInteger();
            int totalRequest = randomIntBetween(throttlingLimit, 3 * throttlingLimit);
            CountDownLatch latch = new CountDownLatch(totalRequest);

            ActionListener listener = new ActionListener() {
                @Override
                public void onResponse(Object o) {
                    latch.countDown();
                    successfulRequest.incrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    throw new AssertionError(e);
                }
            };

            Thread[] threads = new Thread[totalRequest];
            for (int i = 0; i < totalRequest; i++) {
                PutMappingRequest putMappingRequest = new PutMappingRequest("test")
                        .type("type")
                        .source("field" + i, "type=text");
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

            latch.await();
            assertEquals(totalRequest, successfulRequest.get());
            assertEquals(MasterThrottlingRetryListener.getRetryingTasksCount(), 0);
        }
        finally {
            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder()
                    .put("master.throttling.thresholds.put-mapping.value", (String) null)
                    .build();
            settingsRequest.transientSettings(settings);
            assertAcked(client().admin().cluster().updateSettings(settingsRequest).actionGet());
        }
    }

    @BeforeClass
    public static void initTestScheduler() {
        MasterThrottlingRetryListener.setThrottlingRetryScheduler(scheduler);
    }

    @AfterClass
    public static void terminateScheduler() {
        Scheduler.terminate(scheduler, 10, TimeUnit.SECONDS);
    }
}
