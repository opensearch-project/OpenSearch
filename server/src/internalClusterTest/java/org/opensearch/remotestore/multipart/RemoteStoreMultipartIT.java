/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreIT;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RemoteStoreMultipartIT extends RemoteStoreIT {

    Path repositoryLocation;
    boolean compress;
    boolean overrideBuildRepositoryMetadata;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsRepositoryPlugin.TYPE,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsRepositoryPlugin.TYPE
                )
            )
            .build();
    }

    @Before
    public void setup() {
        clusterSettingsSuppliedByTest = true;
        overrideBuildRepositoryMetadata = false;
        repositoryLocation = randomRepoPath();
        compress = randomBoolean();
    }

    @Override
    public RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        if (overrideBuildRepositoryMetadata) {
            Map<String, String> nodeAttributes = node.getAttributes();
            String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

            String settingsAttributeKeyPrefix = String.format(
                Locale.getDefault(),
                REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
                name
            );
            Map<String, String> settingsMap = node.getAttributes()
                .keySet()
                .stream()
                .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
                .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

            Settings.Builder settings = Settings.builder();
            settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
            settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

            if (name.equals(REPOSITORY_NAME)) {
                settings.put("location", repositoryLocation)
                    .put("compress", compress)
                    .put("max_remote_upload_bytes_per_sec", "1kb")
                    .put("chunk_size", 100, ByteSizeUnit.BYTES);
                return new RepositoryMetadata(name, MockFsRepositoryPlugin.TYPE, settings.build());
            }

            return new RepositoryMetadata(name, type, settings.build());
        } else {
            return super.buildRepositoryMetadata(node, name);
        }

    }

    public void testRateLimitedRemoteUploads() throws Exception {
        clusterSettingsSuppliedByTest = true;
        overrideBuildRepositoryMetadata = true;
        Settings.Builder clusterSettings = Settings.builder()
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, repositoryLocation, REPOSITORY_2_NAME, repositoryLocation));
        clusterSettings.put(
            String.format(Locale.getDefault(), "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, REPOSITORY_NAME),
            MockFsRepositoryPlugin.TYPE
        );
        internalCluster().startNode(clusterSettings.build());
        Client client = client();
        logger.info("-->  updating repository");
        Settings.Builder settings = Settings.builder()
            .put("location", repositoryLocation)
            .put("compress", compress)
            .put("max_remote_upload_bytes_per_sec", "1kb")
            .put("chunk_size", 100, ByteSizeUnit.BYTES);
        createRepository(REPOSITORY_NAME, MockFsRepositoryPlugin.TYPE, settings);

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 10; i++) {
            index(INDEX_NAME, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        // check if throttling is active
        assertBusy(() -> {
            long uploadPauseTime = 0L;
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                uploadPauseTime += repositoriesService.repository(REPOSITORY_NAME).getRemoteUploadThrottleTimeInNanos();
            }
            assertThat(uploadPauseTime, greaterThan(TimeValue.timeValueSeconds(randomIntBetween(5, 10)).nanos()));
        }, 30, TimeUnit.SECONDS);

        assertThat(client.prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value, equalTo(10L));
    }
}
