/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;

public class CpuBasedAdmissionControllerTests extends OpenSearchTestCase {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    CpuBasedAdmissionController admissionController = null;
    String action = "TEST_ACTION";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testCheckDefaultParameters() {
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getName(), CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        assertFalse(
            admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode())
        );
    }

    public void testCheckUpdateSettings() {
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            null,
            clusterService,
            Settings.EMPTY
        );
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        clusterService.getClusterSettings().applySettings(settings);

        assertEquals(admissionController.getName(), CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.ENFORCED);
        assertTrue(admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode()));
    }

    public void testApplyControllerWithDefaultSettings() {
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            Settings.EMPTY
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        assertEquals(admissionController.settings.getTransportLayerAdmissionControllerMode(), AdmissionControlMode.DISABLED);
        action = "indices:data/write/bulk[s][p]";
        admissionController.apply(action, AdmissionControlActionType.INDEXING);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
    }

    public void testApplyControllerWhenSettingsEnabled() throws Exception {
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        assertTrue(admissionController.isEnabledForTransportLayer(admissionController.settings.getTransportLayerAdmissionControllerMode()));
        assertTrue(
            admissionController.isAdmissionControllerEnforced(admissionController.settings.getTransportLayerAdmissionControllerMode())
        );
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 0);
        // we can assert admission control and rejections as part of ITs
    }

    public void testRejectionCount() {
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        ResourceUsageCollectorService rs = Mockito.mock(ResourceUsageCollectorService.class);
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            rs,
            clusterService,
            settings
        );
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 1);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 3);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 1);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 3);
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 1);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 2);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.SEARCH.getType()), 2);
        assertEquals(admissionController.getRejectionCount(AdmissionControlActionType.INDEXING.getType()), 5);
    }

    public void testRejectionCountReadsAndWritesConcurrentlyForNewActionType() throws Exception {
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            Mockito.mock(ResourceUsageCollectorService.class),
            clusterService,
            Settings.EMPTY
        );
        final String type = AdmissionControlActionType.FORCE_MERGE.getType();
        // never rejected yet: absent key reads as 0 from both getters
        assertEquals(0, admissionController.getRejectionCount(type));
        assertFalse(admissionController.getRejectionStats().containsKey(type));

        final int writers = randomIntBetween(2, 8);
        final int readers = randomIntBetween(2, 8);
        final int incrementsPerWriter = randomIntBetween(100, 1000);
        final CyclicBarrier barrier = new CyclicBarrier(writers + readers);
        final CountDownLatch done = new CountDownLatch(writers + readers);
        final AtomicBoolean writing = new AtomicBoolean(true);
        final AtomicInteger writersLeft = new AtomicInteger(writers);
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < writers; i++) {
            threadPool.generic().execute(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < incrementsPerWriter; j++) {
                        admissionController.addRejectionCount(type, 1);
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    if (writersLeft.decrementAndGet() == 0) {
                        writing.set(false);
                    }
                    done.countDown();
                }
            });
        }
        for (int i = 0; i < readers; i++) {
            threadPool.generic().execute(() -> {
                try {
                    barrier.await();
                    long previous = 0;
                    while (writing.get()) {
                        // reads race with the first insertion of the key; they must never throw and never go backwards
                        long current = admissionController.getRejectionCount(type);
                        assertTrue(current >= previous);
                        previous = current;
                        Long fromStats = admissionController.getRejectionStats().get(type);
                        assertTrue(fromStats == null || fromStats >= 0);
                    }
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertTrue("unexpected failures: " + failures, failures.isEmpty());
        assertEquals((long) writers * incrementsPerWriter, admissionController.getRejectionCount(type));
        assertEquals(Long.valueOf((long) writers * incrementsPerWriter), admissionController.getRejectionStats().get(type));
    }
}
