/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreIT;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RemoteStoreMultipartIT extends RemoteStoreIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected void putRepository(Path path) {
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME)
                .setType(MockFsRepositoryPlugin.TYPE)
                .setSettings(Settings.builder().put("location", path))
        );
    }

    public void testRateLimitedRemoteUploads() throws Exception {
        internalCluster().startDataOnlyNodes(1);
        Client client = client();
        logger.info("-->  updating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(REPOSITORY_NAME)
                .setType(MockFsRepositoryPlugin.TYPE)
                .setSettings(
                    Settings.builder()
                        .put("location", repositoryLocation)
                        .put("compress", randomBoolean())
                        .put("max_remote_upload_bytes_per_sec", "1kb")
                        .put("chunk_size", 100, ByteSizeUnit.BYTES)
                )
        );

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
