/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RepositoryBlobContainerResolver}, verifying that it resolves a working {@link BlobContainer}
 * from a registered repository's blob store and splits nested and trailing-slash paths correctly.
 */
public class RepositoryBlobContainerResolverTests extends OpenSearchTestCase {

    private static final String REPOSITORY = "repo";

    private Path dataDir;
    private FsBlobStore blobStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        blobStore = new FsBlobStore(8192, dataDir, false);
    }

    private void write(BlobContainer container, String name, String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, true);
        }
    }

    private RepositoriesService serviceWithBasePath(BlobPath basePath) {
        return serviceWithRepository(basePath, true);
    }

    private RepositoriesService serviceWithRepository(BlobPath basePath, boolean readOnly) {
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        when(repository.isReadOnly()).thenReturn(readOnly);
        when(repository.basePath()).thenReturn(basePath);
        when(repository.blobStore()).thenReturn(blobStore);
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository(REPOSITORY)).thenReturn(repository);
        return service;
    }

    private static ObjectStoreSourceConfig urlConfig(String sourceUrl) {
        return new ObjectStoreSourceConfig(Map.of("repository", REPOSITORY, "shard_split", "equal_bytes", "source_url", sourceUrl));
    }

    private static ObjectStoreSourceConfig prefixConfig(String sourcePrefix) {
        return new ObjectStoreSourceConfig(Map.of("repository", REPOSITORY, "shard_split", "whole_files", "source_prefix", sourcePrefix));
    }

    private static String containerRelativeKey(ObjectStoreSourceConfig config, String repositoryRelativeKey) {
        return RepositoryBlobContainerResolver.containerRelativeKey(
            RepositoryBlobContainerResolver.directoryPrefix(config),
            repositoryRelativeKey
        );
    }

    public void testDirectoryPrefixForNestedUrl() {
        assertThat(RepositoryBlobContainerResolver.directoryPrefix(urlConfig("dump/events.ndjson")), equalTo("dump/"));
    }

    public void testDirectoryPrefixForLeafOnlyUrlIsEmpty() {
        assertThat(RepositoryBlobContainerResolver.directoryPrefix(urlConfig("events.ndjson")), equalTo(""));
    }

    public void testDirectoryPrefixForTrailingSlashPrefix() {
        assertThat(RepositoryBlobContainerResolver.directoryPrefix(prefixConfig("dump/")), equalTo("dump/"));
    }

    public void testContainerRelativeKeyStripsDirectoryPrefix() {
        assertThat(containerRelativeKey(prefixConfig("dump/"), "dump/part-0.ndjson"), equalTo("part-0.ndjson"));
    }

    public void testContainerRelativeKeyLeavesLeafOnlyKeyUnchanged() {
        assertThat(containerRelativeKey(urlConfig("events.ndjson"), "events.ndjson"), equalTo("events.ndjson"));
    }

    public void testPrefixForTrailingSlashIsEmpty() {
        assertThat(RepositoryBlobContainerResolver.prefix(prefixConfig("dump/")), equalTo(""));
    }

    public void testPrefixForNestedLeaf() {
        assertThat(RepositoryBlobContainerResolver.prefix(prefixConfig("dump/part")), equalTo("part"));
    }

    public void testPrefixForLeafOnly() {
        assertThat(RepositoryBlobContainerResolver.prefix(prefixConfig("part")), equalTo("part"));
    }

    public void testResolveContainerRootsAtDirectoryForNestedUrl() throws Exception {
        RepositoriesService service = serviceWithBasePath(BlobPath.cleanPath());
        BlobContainer dumpContainer = blobStore.blobContainer(BlobPath.cleanPath().add("dump"));
        write(dumpContainer, "events.ndjson", "r1\nr2\n");

        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("dump/events.ndjson"));
        assertThat(resolved.listBlobsByPrefix("events.ndjson"), hasKey("events.ndjson"));
    }

    public void testResolveContainerHonoursRepositoryBasePath() throws Exception {
        RepositoriesService service = serviceWithBasePath(BlobPath.cleanPath().add("base"));
        BlobContainer baseDump = blobStore.blobContainer(BlobPath.cleanPath().add("base").add("dump"));
        write(baseDump, "part-0.ndjson", "r1\n");

        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, prefixConfig("dump/"));
        assertThat(resolved.listBlobsByPrefix(""), hasKey("part-0.ndjson"));
    }

    public void testResolveContainerRootsAtBasePathForLeafOnlyPath() throws Exception {
        RepositoriesService service = serviceWithBasePath(BlobPath.cleanPath());
        BlobContainer root = blobStore.blobContainer(BlobPath.cleanPath());
        write(root, "events.ndjson", "r1\n");

        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("events.ndjson"));
        assertThat(resolved.listBlobsByPrefix("events.ndjson"), hasKey("events.ndjson"));
    }

    public void testNullServiceRejected() {
        expectThrows(NullPointerException.class, () -> RepositoryBlobContainerResolver.resolveContainer(null, urlConfig("a.ndjson")));
    }

    public void testPrefixIsEmptyWhenSourcePrefixAbsent() {
        assertThat(RepositoryBlobContainerResolver.prefix(urlConfig("dump/events.ndjson")), equalTo(""));
    }

    public void testResolveContainerAtBasePathWhenNoPathConfigured() throws Exception {
        RepositoriesService service = serviceWithBasePath(BlobPath.cleanPath());
        BlobContainer root = blobStore.blobContainer(BlobPath.cleanPath());
        write(root, "events.ndjson", "r1\n");

        ObjectStoreSourceConfig noPath = new ObjectStoreSourceConfig(Map.of("repository", REPOSITORY, "shard_split", "whole_files"));
        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, noPath);
        assertThat(resolved.listBlobsByPrefix("events.ndjson"), hasKey("events.ndjson"));
    }

    public void testResolveContainerSkipsEmptyDirectorySegments() throws Exception {
        RepositoriesService service = serviceWithBasePath(BlobPath.cleanPath());
        BlobContainer dumpContainer = blobStore.blobContainer(BlobPath.cleanPath().add("a").add("b"));
        write(dumpContainer, "events.ndjson", "r1\n");

        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("a//b/events.ndjson"));
        assertThat(resolved.listBlobsByPrefix("events.ndjson"), hasKey("events.ndjson"));
    }

    public void testDirectoryPrefixIsEmptyWhenNoPathConfigured() {
        ObjectStoreSourceConfig noPath = new ObjectStoreSourceConfig(Map.of("repository", REPOSITORY, "shard_split", "whole_files"));
        assertThat(RepositoryBlobContainerResolver.directoryPrefix(noPath), equalTo(""));
    }

    public void testDirectoryPrefixSkipsEmptySegments() {
        assertThat(RepositoryBlobContainerResolver.directoryPrefix(urlConfig("a//b/events.ndjson")), equalTo("a/b/"));
    }

    public void testContainerRelativeKeyLeavesNonMatchingKeyUnchanged() {
        assertThat(containerRelativeKey(urlConfig("dump/events.ndjson"), "other/part-0.ndjson"), equalTo("other/part-0.ndjson"));
    }

    public void testMissingRepositoryRejectedWithClearMessage() {
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository(REPOSITORY)).thenThrow(new RepositoryMissingException(REPOSITORY));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("events.ndjson"))
        );
        assertThat(e.getMessage(), containsString("is not registered"));
        assertThat(e, is(not(instanceOf(UnusableRepositoryException.class))));
    }

    public void testNonBlobStoreRepositoryRejectedWithClearMessage() {
        Repository repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata(REPOSITORY, "fake", Settings.EMPTY));
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository(REPOSITORY)).thenReturn(repository);
        UnusableRepositoryException e = expectThrows(
            UnusableRepositoryException.class,
            () -> RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("events.ndjson"))
        );
        assertThat(e.getMessage(), containsString("not a blob-store repository"));
    }

    public void testReadOnlyRepositoryAccepted() throws Exception {
        RepositoriesService service = serviceWithRepository(BlobPath.cleanPath(), true);
        BlobContainer root = blobStore.blobContainer(BlobPath.cleanPath());
        write(root, "events.ndjson", "r1\n");

        BlobContainer resolved = RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("events.ndjson"));
        assertThat(resolved.listBlobsByPrefix("events.ndjson"), hasKey("events.ndjson"));
    }

    public void testWritableRepositoryRejectedWithClearMessage() {
        RepositoriesService service = serviceWithRepository(BlobPath.cleanPath(), false);
        UnusableRepositoryException e = expectThrows(
            UnusableRepositoryException.class,
            () -> RepositoryBlobContainerResolver.resolveContainer(service, urlConfig("events.ndjson"))
        );
        assertThat(e.getMessage(), containsString("object_store repository [" + REPOSITORY + "] is not read-only"));
        assertThat(e.getMessage(), containsString("\"readonly\": true"));
    }
}
