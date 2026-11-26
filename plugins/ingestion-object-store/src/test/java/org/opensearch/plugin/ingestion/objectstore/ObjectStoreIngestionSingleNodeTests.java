/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * End-to-end single-node ingestion tests for the {@code object_store} source, backed by a registered {@code fs}
 * repository. Records are plain JSON objects; the {@code raw_payload} mapper wraps each line as the document source.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ObjectStoreIngestionSingleNodeTests extends OpenSearchSingleNodeTestCase {

    private static final String INDEX = "object_store_index";
    private static final String REPOSITORY = "object_store_repo";
    private static final String MAPPINGS = "{\"properties\":{\"name\":{\"type\":\"text\"},\"age\":{\"type\":\"integer\"}}}";

    private static Path repoDir;
    private static Path dataDir;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(ObjectStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(Environment.PATH_REPO_SETTING.getKey(), repoDir().toString()).build();
    }

    private Path repoDir() {
        if (repoDir == null) {
            repoDir = createTempDir().resolve("repo");
        }
        return repoDir;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataDir = repoDir().resolve("data");
        Files.createDirectories(dataDir);
        AcknowledgedResponse response = client().admin()
            .cluster()
            .putRepository(
                new PutRepositoryRequest(REPOSITORY).type("fs")
                    .settings(Settings.builder().put("location", dataDir.toString()).put("readonly", true).build())
            )
            .actionGet();
        assertThat(response.isAcknowledged(), is(true));
    }

    private static String doc(String name, int age) {
        return String.format(Locale.ROOT, "{\"name\":\"%s\",\"age\":%d}", name, age);
    }

    private void writeObject(String name, String... records) throws Exception {
        writeObjectWithTerminator(name, "\n", records);
    }

    private void writeObjectWithTerminator(String name, String lineTerminator, String... records) throws Exception {
        Path file = dataDir.resolve(name);
        Files.createDirectories(file.getParent());
        try (
            BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        ) {
            for (String record : records) {
                writer.write(record);
                writer.write(lineTerminator);
            }
            writer.flush();
        }
    }

    private long objectSize(String name) throws Exception {
        return Files.size(dataDir.resolve(name));
    }

    private String manifestEntry(String key) throws Exception {
        return String.format(Locale.ROOT, "{\"key\":\"%s\",\"size\":%d}", key, objectSize(key));
    }

    private Settings.Builder baseSettings(int shards) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("ingestion_source.type", "OBJECT_STORE")
            .put("ingestion_source.pointer.init.reset", "earliest")
            .put("ingestion_source.param.repository", REPOSITORY)
            .put("ingestion_source.param.format", "ndjson")
            .put("ingestion_source.mapper_type", "raw_payload")
            .put("index.replication.type", "SEGMENT");
    }

    private void assertSearchableCount(long expected) throws Exception {
        assertBusy(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(INDEX).setQuery(query).get();
            assertThat(response.getHits().getTotalHits().value(), is(expected));
        });
    }

    private void assertSearchableNames(String... expected) throws Exception {
        assertBusy(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(INDEX).setQuery(query).setSize(100).get();
            Set<String> names = Arrays.stream(response.getHits().getHits())
                .map(hit -> (String) hit.getSourceAsMap().get("name"))
                .collect(Collectors.toSet());
            assertThat(names, is(Set.of(expected)));
        });
    }

    public void testEqualBytesSingleShard() throws Exception {
        writeObject("events.ndjson", doc("alice", 30), doc("bob", 35), doc("carol", 40));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(1).put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "events.ndjson")
                .put("ingestion_source.param.manifest", "[" + manifestEntry("events.ndjson") + "]")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(3);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testEqualBytesTwoShardsSplitsOneObject() throws Exception {
        writeObject("events.ndjson", doc("alice", 30), doc("bob", 35), doc("carol", 40), doc("dave", 45));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "events.ndjson")
                .put("ingestion_source.param.manifest", "[" + manifestEntry("events.ndjson") + "]")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(4);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testWholeFilesPrefix() throws Exception {
        writeObject("part-0.ndjson", doc("alice", 30), doc("bob", 35));
        writeObject("part-1.ndjson", doc("carol", 40));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "whole_files")
                .put("ingestion_source.param.source_prefix", "part-")
                .put("ingestion_source.param.manifest", "[" + manifestEntry("part-0.ndjson") + "," + manifestEntry("part-1.ndjson") + "]")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(3);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testShardPrefixesSplit() throws Exception {
        writeObject("shards/0/part.ndjson", doc("alice", 30), doc("bob", 35));
        writeObject("shards/1/part.ndjson", doc("carol", 40));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "shard_prefixes")
                .put("ingestion_source.param.source_prefix", "shards/")
                .put(
                    "ingestion_source.param.manifest",
                    "[" + manifestEntry("shards/0/part.ndjson") + "," + manifestEntry("shards/1/part.ndjson") + "]"
                )
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(3);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testCrlfRecordsAtExactlyMaxRecordBytesAreIngested() throws Exception {
        String alice = doc("alice", 30);
        String bob = doc("bob", 35);
        int cap = Math.max(alice.length(), bob.length());
        writeObjectWithTerminator("crlf.ndjson", "\r\n", alice, bob);
        createIndexWithMappingSource(
            INDEX,
            baseSettings(1).put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "crlf.ndjson")
                .put("ingestion_source.param.max_record_bytes", cap)
                .put("ingestion_source.param.manifest", "[" + manifestEntry("crlf.ndjson") + "]")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(2);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testCrlfRecordsSplitAcrossTwoShards() throws Exception {
        writeObjectWithTerminator("crlf2.ndjson", "\r\n", doc("alice", 30), doc("bob", 35), doc("carol", 40), doc("dave", 45));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "crlf2.ndjson")
                .put("ingestion_source.param.manifest", "[" + manifestEntry("crlf2.ndjson") + "]")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(4);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testShardPrefixesWithListDiscoveryNeedsNoManifest() throws Exception {
        writeObject("listed/0/part.ndjson", doc("alice", 30), doc("bob", 35));
        writeObject("listed/1/part.ndjson", doc("carol", 40));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "shard_prefixes")
                .put("ingestion_source.param.source_prefix", "listed/")
                .put("ingestion_source.param.discovery", "list")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(3);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testListDiscoveryPicksUpNestedObjectsAndSkipsUnownedPrefixes() throws Exception {
        writeObject("nested/0/a.ndjson", doc("alice", 30));
        writeObject("nested/0/deeper/b.ndjson", doc("bob", 35));
        writeObject("nested/1/c.ndjson", doc("carol", 40));
        writeObject("nested/orphan.ndjson", doc("dave", 45));
        createIndexWithMappingSource(
            INDEX,
            baseSettings(2).put("ingestion_source.param.shard_split", "shard_prefixes")
                .put("ingestion_source.param.source_prefix", "nested/")
                .put("ingestion_source.param.discovery", "list")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
        assertSearchableCount(3);
        assertSearchableNames("alice", "bob", "carol");
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }
}
