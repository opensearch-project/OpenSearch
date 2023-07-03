/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequestBuilder;
import org.opensearch.index.reindex.ReindexTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

public class MultiCodecReindexIT extends ReindexTestCase {

    public void testReindexingMultipleCodecs() throws InterruptedException, ExecutionException {
        internalCluster().ensureAtLeastNumDataNodes(1);
        Map<String, String> codecMap = Map.of(
            "best_compression",
            "BEST_COMPRESSION",
            "zstd_no_dict",
            "ZSTD_NO_DICT",
            "zstd",
            "ZSTD",
            "default",
            "BEST_SPEED"
        );

        for (Map.Entry<String, String> codec : codecMap.entrySet()) {
            assertReindexingWithMultipleCodecs(codec.getKey(), codec.getValue(), codecMap);
        }

    }

    private void assertReindexingWithMultipleCodecs(String destCodec, String destCodecMode, Map<String, String> codecMap)
        throws ExecutionException, InterruptedException {

        final String index = "test-index" + destCodec;
        final String destIndex = "dest-index" + destCodec;

        // creating source index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", "default")
                .put("index.merge.policy.max_merged_segment", "1b")
                .build()
        );
        ensureGreen(index);

        final int nbDocs = randomIntBetween(2, 5);

        // indexing with all 4 codecs
        for (Map.Entry<String, String> codec : codecMap.entrySet()) {
            useCodec(index, codec.getKey());
            ingestDocs(index, nbDocs);
        }

        assertTrue(
            getSegments(index).stream()
                .flatMap(s -> s.getAttributes().values().stream())
                .collect(Collectors.toSet())
                .containsAll(codecMap.values())
        );

        // creating destination index with destination codec
        createIndex(
            destIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", destCodec)
                .build()
        );

        BulkByScrollResponse bulkResponse = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source(index)
            .destination(destIndex)
            .refresh(true)
            .waitForActiveShards(ActiveShardCount.ONE)
            .get();

        assertEquals(codecMap.size() * nbDocs, bulkResponse.getCreated());
        assertEquals(codecMap.size() * nbDocs, bulkResponse.getTotal());
        assertEquals(0, bulkResponse.getDeleted());
        assertEquals(0, bulkResponse.getNoops());
        assertEquals(0, bulkResponse.getVersionConflicts());
        assertEquals(1, bulkResponse.getBatches());
        assertTrue(bulkResponse.getTook().getMillis() > 0);
        assertEquals(0, bulkResponse.getBulkFailures().size());
        assertEquals(0, bulkResponse.getSearchFailures().size());
        assertTrue(getSegments(destIndex).stream().allMatch(segment -> segment.attributes.containsValue(destCodecMode)));
    }

    private void useCodec(String index, String codec) throws ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareClose(index));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(new UpdateSettingsRequest(index).settings(Settings.builder().put("index.codec", codec)))
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index));
    }

    private void flushAndRefreshIndex(String index) {

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock(index, blockSetting);
                // flush
                FlushResponse flushResponse = client().admin().indices().prepareFlush(index).setForce(true).execute().actionGet();
                assertNoFailures(flushResponse);

                // refresh
                RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(index).execute().actionGet();
                assertNoFailures(refreshResponse);
            } finally {
                disableIndexBlock(index, blockSetting);
            }
        }
    }

    private void ingestDocs(String index, int nbDocs) throws InterruptedException {

        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(index).setId(UUID.randomUUID().toString()).setSource("num", i))
                .collect(toList())
        );
        flushAndRefreshIndex(index);
    }

    private ArrayList<Segment> getSegments(String index) {

        return new ArrayList<>(
            client().admin()
                .indices()
                .segments(new IndicesSegmentsRequest(index))
                .actionGet()
                .getIndices()
                .get(index)
                .getShards()
                .get(0)
                .getShards()[0].getSegments()
        );
    }

}
