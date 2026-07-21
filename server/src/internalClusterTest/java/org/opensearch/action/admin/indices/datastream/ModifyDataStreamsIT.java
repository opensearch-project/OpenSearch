/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.DataStream;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end coverage for the {@code POST /_data_stream/_modify} API: metadata-only add/remove of backing indices.
 * A data stream's generation is derived from its backing indices, so it is never set directly. The API is used to
 * re-attach backing indices that exist but are not (or are no longer) part of the stream — for example, indices
 * restored from cold storage that were not re-associated with the stream (opensearch-project/OpenSearch#8271).
 */
public class ModifyDataStreamsIT extends DataStreamTestCase {

    private static final String DS = "logs-modify";

    private List<String> backingIndices() throws Exception {
        return getDataStreams(DS).getDataStreams()
            .get(0)
            .getDataStream()
            .getIndices()
            .stream()
            .map(i -> i.getName())
            .collect(Collectors.toList());
    }

    private long generation() throws Exception {
        return getDataStreams(DS).getDataStreams().get(0).getDataStream().getGeneration();
    }

    private AcknowledgedResponse modify(List<DataStreamAction> actions) throws Exception {
        return client().execute(ModifyDataStreamsAction.INSTANCE, new ModifyDataStreamsAction.Request(actions)).get();
    }

    public void testRemoveBackingIndex() throws Exception {
        createDataStreamIndexTemplate("template", List.of("logs-*"));
        createDataStream(DS);
        rolloverDataStream(DS); // gen 2
        rolloverDataStream(DS); // gen 3
        assertThat(backingIndices(), hasSize(3));

        String firstBackingIndex = DataStream.getDefaultBackingIndexName(DS, 1);
        assertAcked(modify(List.of(DataStreamAction.removeBackingIndex(DS, firstBackingIndex))));

        List<String> after = backingIndices();
        assertThat(after, hasSize(2));
        assertThat(after.contains(firstBackingIndex), equalTo(false));
        // The removed index still exists as a standalone index; it is only detached from the stream.
        assertTrue(client().admin().indices().prepareExists(firstBackingIndex).get().isExists());
        // Generation is unchanged: removing a non-write index does not affect it.
        assertThat(generation(), equalTo(3L));
    }

    public void testCannotRemoveWriteIndex() throws Exception {
        createDataStreamIndexTemplate("template", List.of("logs-*"));
        createDataStream(DS);
        rolloverDataStream(DS);
        String writeIndex = DataStream.getDefaultBackingIndexName(DS, generation());

        Exception e = expectThrows(Exception.class, () -> modify(List.of(DataStreamAction.removeBackingIndex(DS, writeIndex))));
        assertThat(org.opensearch.ExceptionsHelper.unwrapCause(e).getMessage(), containsString("because it is the write index"));
    }

    public void testReattachDetachedBackingIndex() throws Exception {
        // Models the #8271 flow: a backing index is detached (e.g. tiered to cold storage) and later re-attached.
        createDataStreamIndexTemplate("template", List.of("logs-*"));
        createDataStream(DS);
        rolloverDataStream(DS); // gen 2
        rolloverDataStream(DS); // gen 3, backing [1, 2, 3]

        String detached = DataStream.getDefaultBackingIndexName(DS, 1);
        assertAcked(modify(List.of(DataStreamAction.removeBackingIndex(DS, detached))));
        assertThat(backingIndices(), hasSize(2));

        // Re-attach it; the API inserts it back in generation order and leaves the derived generation untouched.
        assertAcked(modify(List.of(DataStreamAction.addBackingIndex(DS, detached))));
        assertThat(
            backingIndices(),
            contains(
                DataStream.getDefaultBackingIndexName(DS, 1),
                DataStream.getDefaultBackingIndexName(DS, 2),
                DataStream.getDefaultBackingIndexName(DS, 3)
            )
        );
        assertThat(generation(), equalTo(3L));
    }

    public void testReattachMarksIndexHidden() throws Exception {
        createDataStreamIndexTemplate("template", List.of("logs-*"));
        createDataStream(DS);
        rolloverDataStream(DS); // gen 2, backing [1, 2]

        String detached = DataStream.getDefaultBackingIndexName(DS, 1);
        assertAcked(modify(List.of(DataStreamAction.removeBackingIndex(DS, detached))));

        // Un-hide the detached index to simulate an index that arrived without the backing-index hidden setting.
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(detached)
                .setSettings(org.opensearch.common.settings.Settings.builder().put("index.hidden", false))
                .get()
        );
        assertThat(
            org.opensearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING.get(
                client().admin().cluster().prepareState().get().getState().metadata().index(detached).getSettings()
            ),
            equalTo(false)
        );

        // Re-attaching it must restore the hidden setting that all backing indices carry.
        assertAcked(modify(List.of(DataStreamAction.addBackingIndex(DS, detached))));
        assertThat(
            org.opensearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING.get(
                client().admin().cluster().prepareState().get().getState().metadata().index(detached).getSettings()
            ),
            equalTo(true)
        );
    }

    public void testRemoveAndReattachMultipleIndicesInSingleRequest() throws Exception {
        createDataStreamIndexTemplate("template", List.of("logs-*"));
        createDataStream(DS);
        rolloverDataStream(DS); // gen 2
        rolloverDataStream(DS); // gen 3
        rolloverDataStream(DS); // gen 4, backing [1, 2, 3, 4]

        String gen1 = DataStream.getDefaultBackingIndexName(DS, 1);
        String gen2 = DataStream.getDefaultBackingIndexName(DS, 2);

        // Remove two non-write indices in one request.
        assertAcked(modify(List.of(DataStreamAction.removeBackingIndex(DS, gen1), DataStreamAction.removeBackingIndex(DS, gen2))));
        assertThat(backingIndices(), hasSize(2));

        // Re-attach both in one request.
        assertAcked(modify(List.of(DataStreamAction.addBackingIndex(DS, gen1), DataStreamAction.addBackingIndex(DS, gen2))));
        assertThat(
            backingIndices(),
            containsInAnyOrder(gen1, gen2, DataStream.getDefaultBackingIndexName(DS, 3), DataStream.getDefaultBackingIndexName(DS, 4))
        );
        assertThat(generation(), equalTo(4L));

        // Rollover still computes the correct next generation, proving generation stayed in sync.
        rolloverDataStream(DS);
        assertThat(generation(), equalTo(5L));
        assertTrue(backingIndices().contains(DataStream.getDefaultBackingIndexName(DS, 5)));
    }
}
