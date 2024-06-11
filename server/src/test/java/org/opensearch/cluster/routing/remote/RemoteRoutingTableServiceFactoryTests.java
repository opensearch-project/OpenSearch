/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class RemoteRoutingTableServiceFactoryTests extends OpenSearchTestCase {

    Supplier<RepositoriesService> repositoriesService;

    public void testGetServiceWhenRemoteRoutingDisabled() {
        Settings settings = Settings.builder().build();
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertTrue(service instanceof NoopRemoteRoutingTableService);
    }

    public void testGetServiceWhenRemoteRoutingEnabled() {
        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), false)
            .build();
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertTrue(service instanceof InternalRemoteRoutingTableService);
    }
}
