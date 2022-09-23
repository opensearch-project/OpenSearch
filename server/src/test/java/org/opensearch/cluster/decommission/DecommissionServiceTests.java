/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.junit.After;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DecommissionServiceTests extends OpenSearchTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(DecommissionServiceTests.class.getName());

    private final TransportService mockTransportService = mock(TransportService.class);

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testClearDecommissionAttribute() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DecommissionService service = new DecommissionService(
                Settings.EMPTY,
                new ClusterService(Settings.EMPTY, settings, threadPool),
                mockTransportService);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.SUCCESSFUL
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build())
            .build();

        final ClusterState newClusterState = service.deleteDecommissionAttribute(clusterState);
        DecommissionAttributeMetadata metadata = newClusterState.metadata().custom(DecommissionAttributeMetadata.TYPE);

        // Decommission Attribute should be removed.
        assertNull(metadata);
    }

    public void testSetWeightForZone() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DecommissionService service = new DecommissionService(
                Settings.EMPTY,
                new ClusterService(Settings.EMPTY, settings, threadPool),
                mockTransportService);

        Map<String, String> weights = Map.of("us-east-1a", "1", "us-east-1b", "1", "us-east-1c", "1");
        service.setWeightForZone(weights);
        verify(mockTransportService).sendRequest(any(), any(), any(), any());
    }
}
