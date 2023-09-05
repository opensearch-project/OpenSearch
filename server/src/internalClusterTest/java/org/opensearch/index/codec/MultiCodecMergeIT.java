/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.Segment;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class MultiCodecMergeIT extends OpenSearchIntegTestCase {

    public void testForceMergeMultipleCodecs() throws ExecutionException, InterruptedException {

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
            forceMergeMultipleCodecs(codec.getKey(), codec.getValue(), codecMap);
        }

    }

    private void forceMergeMultipleCodecs(String finalCodec, String finalCodecMode, Map<String, String> codecMap) throws ExecutionException,
        InterruptedException {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index" + finalCodec;

        // creating index
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
        // ingesting and asserting segment codec mode for all four codecs
        for (Map.Entry<String, String> codec : codecMap.entrySet()) {
            useCodec(index, codec.getKey());
            ingestDocs(index);
        }

        assertTrue(
            getSegments(index).stream()
                .flatMap(s -> s.getAttributes().values().stream())
                .collect(Collectors.toSet())
                .containsAll(codecMap.values())
        );

        // force merge into final codec
        useCodec(index, finalCodec);
        flushAndRefreshIndex(index);
        final ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        assertThat(forceMergeResponse.getFailedShards(), is(0));
        assertThat(forceMergeResponse.getSuccessfulShards(), is(1));

        flushAndRefreshIndex(index);

        List<Segment> segments = getSegments(index).stream().filter(Segment::isSearch).collect(Collectors.toList());
        assertEquals(1, segments.size());
        assertTrue(segments.stream().findFirst().get().attributes.containsValue(finalCodecMode));
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

    private void ingestDocs(String index) throws InterruptedException {
        ingest(index);
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

    private void ingest(String index) throws InterruptedException {

        final int nbDocs = randomIntBetween(1, 5);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(index).setId(UUID.randomUUID().toString()).setSource("num", i))
                .collect(toList())
        );
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
                FlushResponse flushResponse = client().admin().indices().prepareFlush(index).setForce(true).execute().actionGet();
                assertNoFailures(flushResponse);
                RefreshResponse response = client().admin().indices().prepareRefresh(index).execute().actionGet();
                assertNoFailures(response);
            } finally {
                disableIndexBlock(index, blockSetting);
            }
        }
    }

}
