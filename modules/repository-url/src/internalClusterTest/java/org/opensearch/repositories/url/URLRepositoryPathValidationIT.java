/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.url;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.repository.url.URLRepositoryModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end test for URL repository blob path validation.
 *
 * <p>A {@code url}-type repository resolves blob names against its base URL. This test drives the real
 * node's {@link BlobStoreRepository#blobStore()} blob container over a {@code file://} base and verifies
 * that invalid blob paths are rejected while a legitimate in-root read still succeeds.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class URLRepositoryPathValidationIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(URLRepositoryModulePlugin.class);
    }

    public void testUrlRepositoryBlobPathValidation() throws Exception {
        final Client client = client();

        // 1) Produce valid on-disk repository content by taking a real snapshot through an fs repository.
        final Path repositoryLocation = randomRepoPath();
        createRepository("fs-repo", FsRepository.TYPE, Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation));

        createIndex("test-idx");
        ensureGreen();
        for (int i = 0; i < 20; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("fs-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));

        // 2) Plant an out-of-root sentinel that remains under path.repo and is therefore readable by the node.
        final String sentinelName = "out-of-root-sentinel.dat";
        final byte[] sentinel = "OUT-OF-ROOT-SENTINEL".getBytes(StandardCharsets.UTF_8);
        final Path sentinelPath = repositoryLocation.getParent().resolve(sentinelName);
        Files.write(sentinelPath, sentinel);

        // 3) Register the read-only url repository over the file:// base (allowed because it is under path.repo).
        createRepository(
            "url-repo",
            URLRepository.TYPE,
            Settings.builder().put(URLRepository.URL_SETTING.getKey(), repositoryLocation.toUri().toURL().toString())
        );

        // 4) Reach the real repository instance on the cluster-manager node and its root blob container.
        final RepositoriesService repositoriesService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository("url-repo");
        final BlobContainer container = repository.blobStore().blobContainer(repository.basePath());

        // Control: a legitimate in-root blob must still be readable (index.latest always exists post-snapshot).
        try (InputStream in = container.readBlob("index.latest")) {
            assertThat(in.readAllBytes().length, greaterThan(0));
        }

        // 5) Path validation: traversal and absolute names must be rejected before any read.
        final List<String> escapingNames = List.of(
            "../" + sentinelName,
            sentinelPath.toAbsolutePath().toString(),
            "a/../../" + sentinelName
        );
        for (String escaping : escapingNames) {
            final IOException exception = expectThrows(IOException.class, () -> {
                try (InputStream in = container.readBlob(escaping)) {
                    in.readAllBytes();
                }
            });
            assertThat(exception.getMessage(), containsString("invalid blob name"));
        }

        // Repository metadata contributes path elements such as index IDs. Those elements must not
        // change the configured root path, scheme, or authority before the final blob is resolved.
        final List<String> escapingPathElements = List.of(
            "../../" + sentinelName,
            "%2e%2e",
            "//localhost/" + sentinelName,
            "http://localhost/" + sentinelName
        );
        for (String escapingPathElement : escapingPathElements) {
            final BlobPath blobPath = BlobPath.cleanPath().add("indices").add(escapingPathElement);
            final BlobStoreException exception = expectThrows(
                BlobStoreException.class,
                () -> repository.blobStore().blobContainer(blobPath)
            );
            assertThat(exception.getCause().getMessage(), containsString("invalid URL path"));
        }
    }
}
