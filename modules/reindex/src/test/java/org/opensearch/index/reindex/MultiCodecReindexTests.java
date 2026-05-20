/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class MultiCodecReindexTests extends ReindexTestCase {
    final static Map<String, String> codecMap = Map.of(
        "best_compression",
        "BEST_COMPRESSION",
        "zlib",
        "BEST_COMPRESSION",
        "default",
        "BEST_SPEED",
        "lz4",
        "BEST_SPEED"
    );
    final static String[] codecChoices = codecMap.keySet().toArray(String[]::new);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, ReindexModulePlugin.class);
    }

    public void testReindexingMultipleCodecs() throws InterruptedException, ExecutionException {
        for (Map.Entry<String, String> candidate : codecMap.entrySet()) {
            final int nbDocs = randomIntBetween(2, 5);

            final String destCodec = candidate.getKey();
            final String destCodecMode = candidate.getValue();

            final String index = "test-index-" + destCodec;
            final String destIndex = "dest-index-" + destCodec;

            // create source index
            createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", randomFrom(codecChoices))
                    .put(MergePolicyProvider.INDEX_MERGE_ENABLED, false)
                    .build()
            );
            ensureGreen(index);

            // index using all codecs
            for (String codec : codecMap.keySet()) {
                useCodec(index, codec);
                ingestDocs(index, nbDocs);
            }

            assertTrue(
                getSegments(index).stream()
                    .flatMap(s -> s.getAttributes().values().stream())
                    .collect(Collectors.toSet())
                    .containsAll(codecMap.values())
            );

            // create destination index with destination codec
            createIndex(
                destIndex,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", destCodec)
                    .build()
            );
            ensureGreen(destIndex);

            // perform reindex
            BulkByScrollResponse response = reindex().source(index)
                .destination(destIndex)
                .refresh(true)
                .waitForActiveShards(ActiveShardCount.ONE)
                .get();
            final int expectedResponseSize = codecMap.size() * nbDocs;

            // assertions
            assertEquals(0, response.getNoops());
            assertEquals(1, response.getBatches());
            assertEquals(0, response.getDeleted());
            assertEquals(0, response.getVersionConflicts());
            assertEquals(0, response.getBulkFailures().size());
            assertEquals(0, response.getSearchFailures().size());

            assertEquals(expectedResponseSize, response.getTotal());
            assertEquals(expectedResponseSize, response.getCreated());

            assertTrue(response.getTook().getMillis() > 0);
            assertTrue(getSegments(destIndex).stream().allMatch(segment -> segment.attributes.containsValue(destCodecMode)));
        }
    }

    private void useCodec(String index, String codec) throws ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(1));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(new UpdateSettingsRequest(index).settings(Settings.builder().put("index.codec", codec)))
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(1));
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

        flushAndRefresh(index);
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
