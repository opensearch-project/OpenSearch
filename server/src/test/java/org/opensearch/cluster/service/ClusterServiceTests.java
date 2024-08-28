/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.ingest.IngestPipelineValidator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.junit.After;

import static org.hamcrest.Matchers.equalTo;

public class ClusterServiceTests extends OpenSearchTestCase {
    private final TestThreadPool threadPool = new TestThreadPool(ClusterServiceTests.class.getName());

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testDeprecatedGetMasterServiceBWC() {
        try (
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            )
        ) {
            MasterService masterService = clusterService.getMasterService();
            ClusterManagerService clusterManagerService = clusterService.getClusterManagerService();
            assertThat(masterService, equalTo(clusterManagerService));
        }
    }

    public void testUpdateMaxIngestProcessorCountSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // verify defaults
        assertEquals(Integer.MAX_VALUE, clusterSettings.get(IngestPipelineValidator.MAX_NUMBER_OF_INGEST_PROCESSORS).intValue());

        // verify update max processor
        Settings newSettings = Settings.builder().put("cluster.ingest.max_number_processors", 3).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(3, clusterSettings.get(IngestPipelineValidator.MAX_NUMBER_OF_INGEST_PROCESSORS).intValue());
    }
}
