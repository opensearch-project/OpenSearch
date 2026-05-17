/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.awaitility.Awaitility.await;

/**
 * PR5 PoC: end-to-end smoke for the multi-partition consumer + per-partition checkpoint persistence + recovery flow.
 *
 * <p><b>This is throwaway test coverage.</b> Delete this file once the dedicated PR6 integration suite for flexible
 * shard-partition mapping lands. It exists only to validate the PR5 chain (assignment → multi-partition consumer →
 * per-partition checkpoint write → restart → recover via {@code batch_start_p<N>}) before more thorough ITs arrive.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PR5MultiPartitionPoCIT extends KafkaIngestionBaseIT {

    private static final int NUM_PARTITIONS = 4;

    public void testMultiPartitionModuloWithRestart() throws Exception {
        // Re-create the Kafka topic with 4 partitions; the base @Before created a 1-partition topic.
        recreateKafkaTopics(NUM_PARTITIONS);

        // Produce 2 messages to each of the 4 partitions = 8 docs spread across the topic.
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            produceToPartition(p, "p" + p + "-a", "alice" + p, "20");
            produceToPartition(p, "p" + p + "-b", "bob" + p, "30");
        }

        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        // 1 shard + modulo → that shard owns all 4 source partitions.
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.source_partition_strategy", "modulo")
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .build(),
            mapping
        );
        ensureGreen(indexName);
        waitForSearchableDocs(8, Arrays.asList(dataNode));

        // Flush to drive a Lucene commit: this is when per-partition checkpoint keys (batch_start_p0..p3) get written.
        flush(indexName);

        // Restart the data node to force recovery from commit data.
        internalCluster().restartNode(dataNode);
        ensureGreen(indexName);

        // Produce one more message per partition = 4 new docs. Recovery must have positioned each partition at its
        // persisted offset, so we expect exactly 4 NEW docs (no replay of the original 8, no loss of the new 4).
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            produceToPartition(p, "p" + p + "-c", "carol" + p, "40");
        }
        waitForSearchableDocs(12, Arrays.asList(dataNode));

        // Sanity: total doc count is exactly 12 (no missing docs).
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client(dataNode).prepareSearch(indexName)
                .setQuery(new RangeQueryBuilder("age").gte(0))
                .setPreference("_only_local")
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(12L));
        });

        // Critical check: doc count alone cannot detect silent replay because keyed _id updates overwrite. After
        // the restart, the NEW poller's totalPolledCount must be bounded by the at-least-once semantics.
        //
        // Expected breakdown (at-least-once): 4 re-reads + 4 genuinely new = 8.
        // - 4 re-reads: per the existing semantic in MessageProcessorRunnable, currentShardPointer is set
        // BEFORE message processing completes (pre-existing concern, not introduced by PR5). The persisted
        // per-partition checkpoint therefore reflects "last message attempted", and seek(offset N) re-reads
        // offset N on the next poll. Each of the 4 assigned partitions re-reads its last persisted message
        // once. Those re-reads are idempotent overwrites by _id (no new docs), but they ARE polled.
        // - 4 new: the post-restart messages.
        // A higher value (e.g. 12) would indicate full replay of the original 8 pre-restart messages — exactly the
        // Bug 2 (stale perPartitionStartPointers re-seek) and Bug N1 (legacy checkpoint loss) failure modes.
        // Per-poller counters are fresh after restart, so this count reflects only post-restart activity.
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            PollingIngestStats stats = client(dataNode).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
                .getPollingIngestStats();
            assertNotNull(stats);
            long polled = stats.getConsumerStats().totalPolledCount();
            assertThat(
                "post-restart poller polled ["
                    + polled
                    + "] messages; expected <= 8 (4 at-least-once re-reads + 4 new). "
                    + "A value of 12 would indicate full replay (Bug 2 / Bug N1 regression).",
                polled,
                lessThanOrEqualTo(8L)
            );
        });
    }

    /**
     * Reinit-via-settings-update scenario. The {@link #testMultiPartitionModuloWithRestart} test catches recovery
     * issues on engine restart (Bug N1 territory), but Bug 2 — stale {@code perPartitionStartPointers} re-seeked on
     * every reinit — only manifests when {@link org.opensearch.indices.pollingingest.DefaultStreamPoller#requestConsumerReinitialization}
     * fires WITHOUT a restart. That is exactly what a settings update on {@code ingestion_source.param.*} triggers
     * in production. This test exercises that path end-to-end so silent reprocessing on a settings change is caught
     * here, not just in mock-based unit tests.
     */
    public void testMultiPartitionReinitViaSettingsUpdateDoesNotReplay() throws Exception {
        recreateKafkaTopics(NUM_PARTITIONS);
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            produceToPartition(p, "p" + p + "-a", "alice" + p, "20");
            produceToPartition(p, "p" + p + "-b", "bob" + p, "30");
        }

        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.source_partition_strategy", "modulo")
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .build(),
            mapping
        );
        ensureGreen(indexName);
        waitForSearchableDocs(8, Arrays.asList(dataNode));

        // Sanity: the initial 8 messages have all been polled. Capture this as the pre-reinit baseline.
        // Note: NOT flushed — we deliberately want live processor progress with NO persisted recovery, so the
        // only data the reinit's NONE branch can use is the live currentPartitionPointers from PR4. If the NONE
        // branch wrongly used the static perPartitionStartPointers field (empty at startup), the reinit would
        // skip the seek entirely and Kafka would fall through to auto.offset.reset=earliest — re-reading
        // everything from offset 0 (Bug 2).
        long polledBeforeReinit = getPolledCount(dataNode);
        assertThat("pre-reinit baseline: 8 messages polled", polledBeforeReinit, is(8L));

        // Trigger reinit by updating a Kafka consumer param. updateIngestionSourceParams in IngestionEngine
        // (registered on INGESTION_SOURCE_PARAMS_SETTING) calls streamPoller.requestConsumerReinitialization,
        // which sets reinitializeConsumer=true and the next poll loop iteration enters handleConsumerInitialization.
        client(dataNode).admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("ingestion_source.param.fetch.min.bytes", 30001))
            .get();

        // Produce 4 more (1 per partition) and wait for all 12 to be searchable.
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            produceToPartition(p, "p" + p + "-c", "carol" + p, "40");
        }
        waitForSearchableDocs(12, Arrays.asList(dataNode));

        // Critical assertion: post-reinit, the poll-count delta must reflect "live progress preserved across reinit",
        // not "startup recovery re-applied to a consumer with no progress".
        //
        // With Bug 2 FIXED (NONE branch merges perPartitionStartPointers + getCurrentPartitionPointers, live wins):
        // - reinit's seekToPartitionOffsets uses live offsets {0→1, 1→1, 2→1, 3→1}
        // - at-least-once: each partition re-reads its last processed offset → 4 re-reads
        // - + 4 new messages = 8 polls post-reinit
        // - delta from baseline (8) = 8; total = 16
        // With Bug 2 UNFIXED (NONE branch uses only the empty startup perPartitionStartPointers):
        // - seekTo is empty → no seek → Kafka auto.offset.reset=earliest re-reads all 8 originals
        // - + 4 new = 12 polls post-reinit
        // - delta = 12; total = 20
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            long polledAfterReinit = getPolledCount(dataNode);
            long delta = polledAfterReinit - polledBeforeReinit;
            assertThat(
                "post-reinit polled delta ["
                    + delta
                    + "] should be <= 8 (4 at-least-once re-reads + 4 new). "
                    + "A delta of 12 would indicate Bug 2 regression (NONE branch ignoring live progress and "
                    + "falling through to auto.offset.reset).",
                delta,
                lessThanOrEqualTo(8L)
            );
        });
    }

    private long getPolledCount(String node) {
        PollingIngestStats stats = client(node).admin().indices().prepareStats(indexName).get().getIndex(indexName).getShards()[0]
            .getPollingIngestStats();
        assertNotNull(stats);
        return stats.getConsumerStats().totalPolledCount();
    }

    private void produceToPartition(int partition, String id, String name, String age) {
        String payload = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"_op_type\":\"index\",\"_source\":{\"name\":\"%s\",\"age\":%s}}",
            id,
            name,
            age
        );
        producer.send(new ProducerRecord<>(topicName, partition, defaultMessageTimestamp, "null", payload));
    }
}
