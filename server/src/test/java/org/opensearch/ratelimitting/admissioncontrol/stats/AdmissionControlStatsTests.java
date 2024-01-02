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
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class AdmissionControlStatsTests extends OpenSearchTestCase {
    AdmissionController admissionController;
    AdmissionControllerStats admissionControllerStats;
    AdmissionControlStats admissionControlStats;
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
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            mock(ResourceUsageCollectorService.class),
            clusterService,
            settings
        );
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        List<AdmissionControllerStats> admissionControllerStats = new ArrayList<>();
        admissionControlStats = new AdmissionControlStats(admissionControllerStats);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDefaults() throws IOException {
        assertEquals(admissionControlStats.getAdmissionControllerStatsList().size(), 0);
    }

    public void testRejectionCount() throws IOException {
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 11);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 1);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        admissionControlStats = new AdmissionControlStats(List.of(admissionControllerStats));
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder = admissionControlStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String response = builder.toString();
        assertEquals(
            response,
            "{\"admission_control\":{\"global_cpu_usage\":{\"transport\":{\"rejection_count\":{\"search\":11,\"indexing\":1}}}}}"
        );
        AdmissionControlStats admissionControlStats1 = admissionControlStats;
        assertEquals(admissionControlStats.hashCode(), admissionControlStats1.hashCode());
    }
}
