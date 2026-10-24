/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ObjectStoreConsumerFactory} pointer parsing and shard-consumer creation.
 */
public class ObjectStoreConsumerFactoryTests extends OpenSearchTestCase {

    private final ObjectStoreConsumerFactory factory = new ObjectStoreConsumerFactory(() -> null);

    public void testParsePointerFromStringRoundTrips() {
        ObjectStorePointer original = new ObjectStorePointer("obj/key.ndjson", 128);
        assertThat(factory.parsePointerFromString(original.asString()), equalTo(original));
    }

    private static IndexMetadata indexMetadataWith(Map<String, Object> params) {
        return indexMetadataWith(params, null);
    }

    private static IndexMetadata indexMetadataWith(Map<String, Object> params, int numberOfShards) {
        IndexMetadata indexMetadata = indexMetadataWith(params, null);
        when(indexMetadata.getNumberOfShards()).thenReturn(numberOfShards);
        return indexMetadata;
    }

    private static IndexMetadata indexMetadataWith(Map<String, Object> params, IngestionSource.PointerInitReset reset) {
        IngestionSource ingestionSource = mock(IngestionSource.class);
        when(ingestionSource.params()).thenReturn(params);
        when(ingestionSource.getPointerInitReset()).thenReturn(reset);
        when(ingestionSource.getMapperType()).thenReturn(IngestionMessageMapper.MapperType.RAW_PAYLOAD);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIngestionSource()).thenReturn(ingestionSource);
        when(indexMetadata.getNumberOfShards()).thenReturn(1);
        return indexMetadata;
    }

    private static RepositoriesService serviceWithContainer(BlobContainer container) {
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        when(repository.isReadOnly()).thenReturn(true);
        when(repository.basePath()).thenReturn(BlobPath.cleanPath());
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(container);
        when(repository.blobStore()).thenReturn(blobStore);
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository("repo")).thenReturn(repository);
        return service;
    }

    private static IllegalStateException configErrorOnRead(IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer) {
        assertThat(consumer, is(instanceOf(ObjectStoreConfigErrorConsumer.class)));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getMessage(), containsString("is invalid, so ingestion is paused"));
        assertThat(e.getCause(), is(notNullValue()));
        assertThat(e.getMessage(), containsString(e.getCause().getMessage()));
        return e;
    }

    public void testBoundaryResolutionIsChargedToTheCostCaps() throws Exception {
        final Path dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        BlobContainer real = new FsBlobStore(8192, dataDir, false).blobContainer(BlobPath.cleanPath());
        byte[] bytes = "r1\nr2\nr3\nr4\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            real.writeBlob("events.ndjson", in, bytes.length, true);
        }
        RepositoriesService service = serviceWithContainer(real);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "max_get_requests",
            1L,
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":" + bytes.length + "}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 1, indexMetadataWith(params, 3)));
        assertThat(e.getCause(), is(instanceOf(CostLimitExceededException.class)));
        assertThat(e.getMessage(), containsString("max_get_requests"));
    }

    public void testCreateShardConsumerBuildsFromManifestParam() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(params)
        );
        assertThat(consumer, is(notNullValue()));
        assertThat(consumer.getShardId(), is(0));
        assertThat(consumer.earliestPointer(), equalTo(new ObjectStorePointer("events.ndjson", 0)));
        assertThat(consumer.latestPointer(), equalTo(new ObjectStorePointer("events.ndjson", 52)));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(52L));
    }

    public void testCreateShardConsumerRejectsInvalidManifest() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"other.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 0, indexMetadataWith(params)));
        assertThat(e.getMessage(), containsString("source_url"));
    }

    public void testCreateShardConsumerRejectsLatestReset() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IngestionSource.PointerInitReset latest = new IngestionSource.PointerInitReset(StreamPoller.ResetState.LATEST, "");

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 0, indexMetadataWith(params, latest)));
        assertThat(e.getMessage(), containsString("latest"));
    }

    public void testCreateShardConsumerRejectsFieldMappingMapper() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IngestionSource ingestionSource = mock(IngestionSource.class);
        when(ingestionSource.params()).thenReturn(params);
        when(ingestionSource.getPointerInitReset()).thenReturn(null);
        when(ingestionSource.getMapperType()).thenReturn(IngestionMessageMapper.MapperType.FIELD_MAPPING);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIngestionSource()).thenReturn(ingestionSource);
        when(indexMetadata.getNumberOfShards()).thenReturn(1);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 0, indexMetadata));
        assertThat(e.getMessage(), containsString("field_mapping"));
    }

    public void testCreateShardConsumerRejectsDefaultMapper() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IndexMetadata indexMetadata = indexMetadataWith(params);
        when(indexMetadata.getIngestionSource().getMapperType()).thenReturn(IngestionMessageMapper.MapperType.DEFAULT);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 0, indexMetadata));
        assertThat(e.getMessage(), containsString("mapper_type [default]"));
        assertThat(e.getMessage(), containsString("index.ingestion_source.mapper_type: raw_payload"));
    }

    public void testCreateShardConsumerAcceptsEarliestReset() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IngestionSource.PointerInitReset earliest = new IngestionSource.PointerInitReset(StreamPoller.ResetState.EARLIEST, "");

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(params, earliest)
        );
        assertThat(consumer, is(notNullValue()));
        assertThat(consumer.earliestPointer(), equalTo(new ObjectStorePointer("events.ndjson", 0)));
        assertThat(consumer.getPointerBasedLag(consumer.earliestPointer()), is(52L));
    }

    public void testCreateShardConsumerRejectsIndexLevelResetByOffsetForMultipleShards() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "whole_files",
            "source_prefix",
            "dump/",
            "manifest",
            "[{\"key\":\"dump/a.ndjson\",\"size\":10},{\"key\":\"dump/b.ndjson\",\"size\":10}]"
        );
        IngestionSource.PointerInitReset byOffset = new IngestionSource.PointerInitReset(
            StreamPoller.ResetState.RESET_BY_OFFSET,
            new ObjectStorePointer("a.ndjson", 0).asString()
        );
        IndexMetadata indexMetadata = indexMetadataWith(params, byOffset);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 1, indexMetadata));
        assertThat(e.getMessage(), containsString("reset_by_offset"));
        assertThat(e.getMessage(), containsString("[2] shards and [shard_split=whole_files]"));
    }

    public void testCreateShardConsumerAcceptsIndexLevelResetByOffsetForSingleShard() {
        BlobContainer container = mock(BlobContainer.class);
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IngestionSource.PointerInitReset byOffset = new IngestionSource.PointerInitReset(
            StreamPoller.ResetState.RESET_BY_OFFSET,
            new ObjectStorePointer("events.ndjson", 10).asString()
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(params, byOffset)
        );
        assertThat(consumer.pointerFromOffset(byOffset.getValue()), equalTo(new ObjectStorePointer("events.ndjson", 10)));
    }

    public void testCreateShardConsumerWrapsSliceResolutionFailure() throws Exception {
        BlobContainer container = mock(BlobContainer.class);
        when(container.readBlob(anyString(), anyLong(), anyLong())).thenThrow(new IOException("read failed"));
        RepositoriesService service = serviceWithContainer(container);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );
        IndexMetadata indexMetadata = indexMetadataWith(params);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        UncheckedIOException e = expectThrows(
            UncheckedIOException.class,
            () -> serviceFactory.createShardConsumer("client", 1, indexMetadata)
        );
        assertThat(e.getMessage(), containsString("failed to resolve slices for shard 1"));
        assertThat(e.getCause(), is(not(instanceOf(UnresolvableBoundaryException.class))));
    }

    private static BlobContainer fsContainerWith(Path dataDir, String name, String content) throws IOException {
        Files.createDirectories(dataDir);
        BlobContainer container = new FsBlobStore(8192, dataDir, false).blobContainer(BlobPath.cleanPath());
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, true);
        }
        return container;
    }

    private static Map<String, Object> equalBytesParams(long declaredSize, long maxRecordBytes) {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "equal_bytes");
        params.put("source_url", "events.ndjson");
        params.put("manifest", "[{\"key\":\"events.ndjson\",\"size\":" + declaredSize + "}]");
        params.put("max_record_bytes", maxRecordBytes);
        return params;
    }

    public void testUnsplittableEqualBytesObjectYieldsConfigErrorConsumer() throws Exception {
        String content = "{\"name\":\"a\",\"age\":1}\n{\"name\":\"" + "x".repeat(60) + "\",\"age\":2}\n{\"name\":\"c\",\"age\":3}\n";
        BlobContainer container = fsContainerWith(createTempDir().resolve("data"), "events.ndjson", content);
        RepositoriesService service = serviceWithContainer(container);
        IndexMetadata indexMetadata = indexMetadataWith(equalBytesParams(content.length(), 30L));
        when(indexMetadata.getNumberOfShards()).thenReturn(4);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 1, indexMetadata));
        assertThat(e.getCause(), is(instanceOf(UnresolvableBoundaryException.class)));
        assertThat(e.getMessage(), containsString("is not splittable ndjson"));
        assertThat(serviceFactory.createShardConsumer("client", 3, indexMetadata), is(instanceOf(ObjectStoreShardConsumer.class)));
    }

    public void testTruncatedEqualBytesObjectYieldsConfigErrorConsumer() throws Exception {
        BlobContainer container = fsContainerWith(createTempDir().resolve("data"), "events.ndjson", "aaaa\nbbbb\n");
        RepositoriesService service = serviceWithContainer(container);
        IndexMetadata indexMetadata = indexMetadataWith(equalBytesParams(100L, 30L));
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 1, indexMetadata));
        assertThat(e.getCause(), is(instanceOf(UnresolvableBoundaryException.class)));
        assertThat(e.getMessage(), containsString("the object was truncated"));
    }

    private static Map<String, Object> listParams(Map<String, Object> extra) {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "shard_prefixes");
        params.put("source_prefix", "dump/");
        params.put("discovery", "list");
        params.putAll(extra);
        return params;
    }

    public void testConfigErrorDoesNotResolveTheRepository() {
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":"
        );
        ObjectStoreConsumerFactory unreachableFactory = new ObjectStoreConsumerFactory(() -> {
            throw new AssertionError("the repository must not be resolved for a configuration error");
        });

        IllegalStateException e = configErrorOnRead(unreachableFactory.createShardConsumer("client", 0, indexMetadataWith(params)));
        assertThat(e.getCause(), is(instanceOf(IllegalArgumentException.class)));
        assertThat(e.getMessage(), containsString("[manifest] is not valid JSON"));
    }

    public void testUnregisteredRepositoryStillThrowsSoCreationIsRetried() {
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository("repo")).thenThrow(new RepositoryMissingException("repo"));
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> serviceFactory.createShardConsumer("client", 0, indexMetadataWith(params))
        );
        assertThat(e.getMessage(), containsString("is not registered"));
        assertThat(e, is(not(instanceOf(UnusableRepositoryException.class))));
    }

    public void testNonStringParamYieldsConfigErrorConsumer() {
        Map<String, Object> params = Map.of(
            "repository",
            List.of("repo-a", "repo-b"),
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> mock(RepositoriesService.class));
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(params)
        );

        configErrorOnRead(consumer);
    }

    public void testWritableRepositoryYieldsConfigErrorConsumerWithoutIo() {
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        when(repository.isReadOnly()).thenReturn(false);
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository("repo")).thenReturn(repository);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(params)
        );
        IllegalStateException e = configErrorOnRead(consumer);
        assertThat(e.getCause(), is(instanceOf(UnusableRepositoryException.class)));
        assertThat(e.getMessage(), containsString("object_store repository [repo] is not read-only"));
        verify(repository).isReadOnly();
        verifyNoMoreInteractions(repository);
    }

    public void testNonBlobStoreRepositoryYieldsConfigErrorConsumer() {
        Repository repository = mock(Repository.class);
        when(repository.getMetadata()).thenReturn(new RepositoryMetadata("repo", "fake", Settings.EMPTY));
        RepositoriesService service = mock(RepositoriesService.class);
        when(service.repository("repo")).thenReturn(repository);
        Map<String, Object> params = Map.of(
            "repository",
            "repo",
            "shard_split",
            "equal_bytes",
            "source_url",
            "events.ndjson",
            "manifest",
            "[{\"key\":\"events.ndjson\",\"size\":52}]"
        );

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(serviceFactory.createShardConsumer("client", 0, indexMetadataWith(params)));
        assertThat(e.getCause(), is(instanceOf(UnusableRepositoryException.class)));
        assertThat(e.getMessage(), containsString("not a blob-store repository"));
    }

    public void testListCostCapExceededYieldsConfigErrorConsumerWithoutFurtherIo() throws Exception {
        BlobContainer container = mock(BlobContainer.class);
        BlobContainer shardContainer = mock(BlobContainer.class);
        when(container.children()).thenReturn(Map.of("0", shardContainer));
        when(shardContainer.listBlobs()).thenReturn(Map.of());
        RepositoriesService service = serviceWithContainer(container);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> consumer = serviceFactory.createShardConsumer(
            "client",
            0,
            indexMetadataWith(listParams(Map.of("max_list_requests", 1L)))
        );
        IllegalStateException e = configErrorOnRead(consumer);
        assertThat(e.getCause(), is(instanceOf(CostLimitExceededException.class)));
        assertThat(e.getMessage(), containsString("exceeds max_list_requests [1]"));

        expectThrows(IllegalStateException.class, () -> consumer.readNext(new ObjectStorePointer("0/a.ndjson", 0), true, 10, 1000));
        assertThat(consumer.getPointerBasedLag(null), is(ObjectStoreConfigErrorConsumer.UNKNOWN_LAG));
        verify(container, times(1)).children();
        verify(shardContainer, times(1)).listBlobs();
        verifyNoMoreInteractions(container, shardContainer);
    }

    public void testListMaxObjectsExceededYieldsConfigErrorConsumer() throws Exception {
        BlobContainer container = mock(BlobContainer.class);
        BlobContainer shardContainer = mock(BlobContainer.class);
        when(container.children()).thenReturn(Map.of("0", shardContainer));
        when(shardContainer.listBlobs()).thenReturn(
            Map.of("a.ndjson", new PlainBlobMetadata("a.ndjson", 10), "b.ndjson", new PlainBlobMetadata("b.ndjson", 10))
        );
        RepositoriesService service = serviceWithContainer(container);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        IllegalStateException e = configErrorOnRead(
            serviceFactory.createShardConsumer("client", 0, indexMetadataWith(listParams(Map.of("max_objects", 1L))))
        );
        assertThat(e.getCause(), is(instanceOf(IllegalArgumentException.class)));
        assertThat(e.getMessage(), containsString("found more than [max_objects=1]"));
    }

    public void testListIoErrorStillThrowsSoCreationIsRetried() throws Exception {
        BlobContainer container = mock(BlobContainer.class);
        when(container.children()).thenThrow(new IOException("list failed"));
        RepositoriesService service = serviceWithContainer(container);

        ObjectStoreConsumerFactory serviceFactory = new ObjectStoreConsumerFactory(() -> service);
        UncheckedIOException e = expectThrows(
            UncheckedIOException.class,
            () -> serviceFactory.createShardConsumer("client", 0, indexMetadataWith(listParams(Map.of())))
        );
        assertThat(e.getMessage(), containsString("failed to resolve slices for shard 0"));
    }
}
