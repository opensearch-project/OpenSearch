/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.cluster.DataStreamTestHelper.createBackingIndex;
import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MetadataDataStreamsServiceTests extends OpenSearchTestCase {

    private static final String DS = "logs-foo";

    /**
     * Builds a cluster state whose data stream {@code DS} has backing indices for exactly the given generations (the
     * highest of which must equal {@code generation}, so the write index follows the naming convention), plus any
     * {@code standaloneIndices} that exist as plain indices but are NOT part of the stream. Every standalone backing
     * index counter must be {@code <= generation}, otherwise {@link Metadata.Builder#build()} validation rejects it as a
     * would-be rollover conflict.
     */
    private ClusterState state(long generation, List<Integer> backingGenerations, int... standaloneIndices) {
        Metadata.Builder metadata = Metadata.builder();
        List<Index> streamIndices = new ArrayList<>();
        for (int gen : backingGenerations) {
            IndexMetadata im = createBackingIndex(DS, gen).build();
            metadata.put(im, false);
            streamIndices.add(im.getIndex());
        }
        for (int gen : standaloneIndices) {
            metadata.put(createBackingIndex(DS, gen).build(), false);
        }
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), streamIndices, generation));
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    private static List<String> backingIndexNames(ClusterState state) {
        return state.metadata().dataStreams().get(DS).getIndices().stream().map(Index::getName).collect(Collectors.toList());
    }

    public void testAddBackingIndexReinsertsInGenerationOrder() {
        // Stream foo is at gen 3 with backing [1, 3] (gen 2 was previously detached and still exists standalone).
        ClusterState state = state(3, List.of(1, 3), 2);
        String reAdded = DataStream.getDefaultBackingIndexName(DS, 2);

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex(DS, reAdded)));

        // The re-added index lands in generation order, not at the end.
        assertThat(
            backingIndexNames(updated),
            contains(
                DataStream.getDefaultBackingIndexName(DS, 1),
                DataStream.getDefaultBackingIndexName(DS, 2),
                DataStream.getDefaultBackingIndexName(DS, 3)
            )
        );
        // Generation is derived and unchanged: the write index is still gen 3.
        assertThat(updated.metadata().dataStreams().get(DS).getGeneration(), equalTo(3L));
    }

    public void testAddBackingIndexIsIdempotent() {
        ClusterState state = state(2, List.of(1, 2));
        String existing = DataStream.getDefaultBackingIndexName(DS, 2);

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex(DS, existing)));

        assertThat(backingIndexNames(updated).size(), equalTo(2));
        assertThat(updated.metadata().dataStreams().get(DS).getGeneration(), equalTo(2L));
    }

    public void testAddUnknownIndexFails() {
        ClusterState state = state(2, List.of(1, 2));
        String missing = DataStream.getDefaultBackingIndexName(DS, 5);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex(DS, missing)))
        );
        assertThat(e.getMessage(), containsString("not found"));
    }

    public void testAddIndexNotFollowingNamingConventionFails() {
        // A plain index that does not match the .ds-<dataStream>-NNNNNN convention.
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata backing = createBackingIndex(DS, 1).build();
        metadata.put(backing, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(backing.getIndex()), 1));
        metadata.put(
            IndexMetadata.builder("random-index")
                .settings(org.opensearch.common.settings.Settings.builder().put("index.version.created", org.opensearch.Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            false
        );
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex(DS, "random-index")))
        );
        assertThat(e.getMessage(), containsString("does not follow the backing index naming convention"));
    }

    public void testRemoveBackingIndex() {
        ClusterState state = state(3, List.of(1, 2, 3));
        String toRemove = DataStream.getDefaultBackingIndexName(DS, 1);

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex(DS, toRemove))
        );

        assertThat(
            backingIndexNames(updated),
            contains(DataStream.getDefaultBackingIndexName(DS, 2), DataStream.getDefaultBackingIndexName(DS, 3))
        );
        // Generation is unaffected by removing a non-write index.
        assertThat(updated.metadata().dataStreams().get(DS).getGeneration(), equalTo(3L));
    }

    public void testRemoveWriteIndexFails() {
        ClusterState state = state(3, List.of(1, 2, 3));
        String writeIndex = DataStream.getDefaultBackingIndexName(DS, 3);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.removeBackingIndex(DS, writeIndex)))
        );
        assertThat(e.getMessage(), containsString("because it is the write index"));
    }

    public void testRemoveThenReAddComposeInSingleUpdate() {
        ClusterState state = state(3, List.of(1, 2, 3));
        String middle = DataStream.getDefaultBackingIndexName(DS, 2);

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex(DS, middle), DataStreamAction.addBackingIndex(DS, middle))
        );

        DataStream ds = updated.metadata().dataStreams().get(DS);
        assertThat(ds.getIndices().size(), equalTo(3));
        assertThat(ds.getGeneration(), equalTo(3L));
    }

    public void testAddMarksBackingIndexHidden() {
        // A standalone, non-hidden backing-index-named index at a counter <= generation.
        String DS_NAME = DS;
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata b1 = createBackingIndex(DS_NAME, 1).build();
        IndexMetadata b2 = createBackingIndex(DS_NAME, 2).build();
        metadata.put(b1, false);
        metadata.put(b2, false);
        // gen 1 exists standalone and is explicitly NOT hidden
        IndexMetadata visible = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(DS_NAME, 1))
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put("index.version.created", org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, false)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        // Stream at gen 2 with backing [2] only; gen-1 index is standalone and visible.
        metadata.put(visible, true);
        metadata.put(new DataStream(DS_NAME, createTimestampField("@timestamp"), List.of(b2.getIndex()), 2));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        String toAdd = DataStream.getDefaultBackingIndexName(DS_NAME, 1);
        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(state.metadata().index(toAdd).getSettings()), equalTo(false));

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.addBackingIndex(DS_NAME, toAdd))
        );

        // The attached index is now hidden, matching every other backing index.
        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(updated.metadata().index(toAdd).getSettings()), equalTo(true));
        assertThat(backingIndexNames(updated), contains(toAdd, DataStream.getDefaultBackingIndexName(DS_NAME, 2)));
    }

    public void testUnknownDataStreamFails() {
        ClusterState state = state(2, List.of(1, 2));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                List.of(DataStreamAction.removeBackingIndex("no-such-stream", ".ds-no-such-stream-000001"))
            )
        );
        assertThat(e.getMessage(), containsString("data stream [no-such-stream] not found"));
    }
}
