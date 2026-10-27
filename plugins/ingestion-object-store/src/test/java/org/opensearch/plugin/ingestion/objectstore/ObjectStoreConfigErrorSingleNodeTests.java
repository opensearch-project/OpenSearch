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
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.client.Requests;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ObjectStoreConfigErrorSingleNodeTests extends OpenSearchSingleNodeTestCase {

    private static final String INDEX = "object_store_config_error_index";
    private static final String REPOSITORY = "object_store_config_error_repo";
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

    private long writeObject(String name, String content) throws Exception {
        Path file = dataDir.resolve(name);
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return Files.size(file);
    }

    private void createIndexWithFormat(String format, long size) {
        createIndexWithMappingSource(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "OBJECT_STORE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.repository", REPOSITORY)
                .put("ingestion_source.param.format", format)
                .put("ingestion_source.param.shard_split", "equal_bytes")
                .put("ingestion_source.param.source_url", "events.ndjson")
                .put("ingestion_source.param.manifest", String.format(Locale.ROOT, "[{\"key\":\"events.ndjson\",\"size\":%d}]", size))
                .put("ingestion_source.mapper_type", "raw_payload")
                .put("index.replication.type", "SEGMENT")
                .build(),
            MAPPINGS
        );
        ensureGreen(INDEX);
    }

    private ShardIngestionState shardState() {
        GetIngestionStateResponse response = client().admin()
            .indices()
            .getIngestionState(Requests.getIngestionStateRequest(INDEX))
            .actionGet();
        assertThat(response.getShardStates().length, is(1));
        return response.getShardStates()[0];
    }

    private PollingIngestStats pollingStats() {
        return client().admin().indices().prepareStats(INDEX).get().getIndex(INDEX).getShards()[0].getPollingIngestStats();
    }

    private long searchableCount() {
        client().admin().indices().prepareRefresh(INDEX).get();
        SearchResponse response = client().prepareSearch(INDEX).setQuery(new RangeQueryBuilder("age").gte(0)).get();
        return response.getHits().getTotalHits().value();
    }

    public void testConfigurationErrorPausesIngestion() throws Exception {
        long size = writeObject("events.ndjson", "{\"name\":\"alice\",\"age\":30}\n");
        createIndexWithFormat("csv", size);

        assertBusy(() -> {
            ShardIngestionState state = shardState();
            assertThat(state.isPollerPaused(), is(true));
            assertThat(state.getPollerState(), is("PAUSED"));
            assertThat(pollingStats().getConsumerStats().totalConsumerErrorCount(), greaterThanOrEqualTo(1L));
        });
        assertThat(searchableCount(), is(0L));
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }

    public void testFixingTheParamsAndResumingRecovers() throws Exception {
        long size = writeObject("events.ndjson", "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":35}\n");
        createIndexWithFormat("csv", size);
        assertBusy(() -> assertThat(shardState().isPollerPaused(), is(true)));

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX)
            .setSettings(Settings.builder().put("index.ingestion_source.param.format", "ndjson"))
            .get();
        client().admin().indices().resumeIngestion(Requests.resumeIngestionRequest(INDEX)).actionGet();

        assertBusy(() -> assertThat(searchableCount(), is(2L)));
        client().admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
    }
}
