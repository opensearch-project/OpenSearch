/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.stats;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class AdmissionControllerStatsTests extends OpenSearchTestCase {
    AdmissionController admissionController;
    AdmissionControllerStats admissionControllerStats;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        admissionController = new CpuBasedAdmissionController("TEST", mock(ResourceUsageCollectorService.class), clusterService, settings);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDefaults() throws IOException {
        assertEquals(admissionControllerStats.getRejectionCount().size(), 0);
        assertEquals(admissionControllerStats.getAdmissionControllerName(), "TEST");
    }

    public void testRejectionCount() throws IOException {
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 11);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 1);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        long searchRejection = admissionControllerStats.getRejectionCount().getOrDefault(AdmissionControlActionType.SEARCH.getType(), 0L);
        long indexingRejection = admissionControllerStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.INDEXING.getType(), 0L);
        assertEquals(searchRejection, 11);
        assertEquals(indexingRejection, 1);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = admissionControllerStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String response = builder.toString();
        assertEquals(response, "{\"transport\":{\"rejection_count\":{\"search\":11,\"indexing\":1}}}");
        AdmissionControllerStats admissionControllerStats1 = admissionControllerStats;
        assertEquals(admissionControllerStats.hashCode(), admissionControllerStats1.hashCode());
    }
}
