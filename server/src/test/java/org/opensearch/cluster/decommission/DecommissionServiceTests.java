/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.hamcrest.MatcherAssert;
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

import static org.hamcrest.Matchers.*;

public class DecommissionServiceTests extends OpenSearchTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(DecommissionServiceTests.class.getName());

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testAddRecommissionAttributeToClusterWithWrongRecommission() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DecommissionService service = new DecommissionService(new ClusterService(Settings.EMPTY, settings, threadPool));
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute, DecommissionStatus.DECOMMISSION_SUCCESSFUL);
        final DecommissionAttribute recommissionAttribute = new DecommissionAttribute("zone", "zone-3");
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
                .metadata(Metadata.builder()
                .putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build())
                .build();

        DecommissionFailedException e = expectThrows(DecommissionFailedException.class, () -> service.addRecommissionAttributeToCluster(clusterState, recommissionAttribute));
        MatcherAssert.assertThat(e.getMessage(), containsString("Recommission only allowed for decommissioned zone"));
    }

    public void testAddRecommissionAttributeToCluster() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DecommissionService service = new DecommissionService(new ClusterService(Settings.EMPTY, settings, threadPool));
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute, DecommissionStatus.DECOMMISSION_SUCCESSFUL);
        final DecommissionAttribute recommissionAttribute = new DecommissionAttribute("zone", "zone-2");
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
                .metadata(Metadata.builder()
                        .putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build())
                .build();

        final ClusterState newClusterState = service.addRecommissionAttributeToCluster(clusterState, recommissionAttribute);
        DecommissionAttributeMetadata metadata = newClusterState.metadata().custom(DecommissionAttributeMetadata.TYPE);
        MatcherAssert.assertThat(metadata.status(), is(DecommissionStatus.RECOMMISSION_IN_PROGRESS));
    }
}
