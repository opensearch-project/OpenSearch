/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.List;

import static org.opensearch.cluster.DataStreamTestHelper.getClusterStateWithDataStreams;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class ModifyDataStreamsActionTests extends org.opensearch.test.OpenSearchTestCase {

    private static final String DATA_STREAM = "my-data-stream";

    /**
     * Builds a {@link MapperService} for an arbitrary index so the add-backing-index path can merge the
     * {@code _data_stream_timestamp} meta field into it without a running node.
     */
    private CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory() {
        return indexMetadata -> MapperTestUtils.newMapperService(
            xContentRegistry(),
            createTempDir(),
            indexMetadata.getSettings(),
            indexMetadata.getIndex().getName()
        );
    }

    private ClusterState modify(ClusterState cs, List<DataStreamAction> actions) {
        try {
            return ModifyDataStreamsAction.modifyDataStream(cs, actions, mapperServiceFactory());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testAddBackingIndex() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));

        ClusterState newState = modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index")));

        DataStream ds = newState.metadata().dataStreams().get(DATA_STREAM);
        assertThat(ds.getIndices().size(), equalTo(3));
        // new backing index is added to the front so the existing write index stays last
        assertThat(ds.getIndices().get(0).getName(), equalTo("standalone-index"));
        assertThat(
            ds.getIndices().get(ds.getIndices().size() - 1).getName(),
            equalTo(DataStream.getDefaultBackingIndexName(DATA_STREAM, 2))
        );
        // generation is unchanged when adding a backing index
        assertThat(ds.getGeneration(), equalTo(2L));
        // the added index is now hidden
        assertTrue(IndexMetadata.INDEX_HIDDEN_SETTING.get(newState.metadata().index("standalone-index").getSettings()));
    }

    public void testAddBackingIndexMergesDataStreamTimestampMetaField() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));

        ClusterState newState = modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index")));

        // the added index has had the _data_stream_timestamp meta field merged into its mapping (auto-adapt)
        IndexMetadata added = newState.metadata().index("standalone-index");
        assertNotNull(added.mapping());
        assertThat(added.mapping().source().string(), containsString("_data_stream_timestamp"));
    }

    public void testAddBackingIndexWithExistingMappingMergesTimestampMetaField() throws IOException {
        ClusterState base = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of());
        // a standalone index that already has a mapping with a plain @timestamp field (but no _data_stream_timestamp
        // meta field), mirroring an externally created index being adopted as a backing index
        IndexMetadata mapped = IndexMetadata.builder("mapped-index")
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}")
            .build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder(base.metadata()).put(mapped, false))
            .build();

        ClusterState newState = modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "mapped-index")));

        DataStream ds = newState.metadata().dataStreams().get(DATA_STREAM);
        assertThat(ds.getIndices().stream().map(i -> i.getName()).toList(), hasItem("mapped-index"));
        IndexMetadata added = newState.metadata().index("mapped-index");
        assertThat(added.mapping().source().string(), containsString("_data_stream_timestamp"));
        assertTrue(IndexMetadata.INDEX_HIDDEN_SETTING.get(added.getSettings()));
    }

    public void testReAddBackingIndexOnlyBumpsSettingsVersion() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));

        // add the standalone index: it is adapted (hidden + _data_stream_timestamp mapping merged in)
        ClusterState afterAdd = modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index")));
        // remove it again: it stays in the cluster state with its (now canonical) mapping but is un-hidden
        ClusterState afterRemove = modify(afterAdd, List.of(DataStreamAction.removeBackingIndex(DATA_STREAM, "standalone-index")));
        IndexMetadata adapted = afterRemove.metadata().index("standalone-index");

        // re-add it: its mapping already carries the _data_stream_timestamp meta field, so re-adapting must not change
        // the mapping, but hiding it again does change the settings (this is the testRemoveThenAddBackingIndex IT case)
        ClusterState afterReAdd = modify(afterRemove, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index")));

        IndexMetadata reAdded = afterReAdd.metadata().index("standalone-index");
        assertThat(
            afterReAdd.metadata().dataStreams().get(DATA_STREAM).getIndices().stream().map(i -> i.getName()).toList(),
            hasItem("standalone-index")
        );
        assertTrue(IndexMetadata.INDEX_HIDDEN_SETTING.get(reAdded.getSettings()));
        // mapping is unchanged on re-add, so the mapping version must NOT move (node asserts bump implies content change)
        assertThat(reAdded.getMappingVersion(), equalTo(adapted.getMappingVersion()));
        // but hiding the index changed its settings, so the settings version MUST move
        assertThat(reAdded.getSettingsVersion(), equalTo(adapted.getSettingsVersion() + 1));
    }

    public void testAddBackingIndexBumpsSettingsVersion() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));
        long beforeVersion = cs.metadata().index("standalone-index").getSettingsVersion();

        ClusterState newState = modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index")));

        // hidden flips false -> true, so the settings version must be bumped (IndexService.updateMetadata asserts this)
        assertThat(newState.metadata().index("standalone-index").getSettingsVersion(), equalTo(beforeVersion + 1));
    }

    public void testRemoveBackingIndex() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 3)), List.of());
        String indexToRemove = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);
        long beforeVersion = cs.metadata().index(indexToRemove).getSettingsVersion();

        ClusterState newState = modify(cs, List.of(DataStreamAction.removeBackingIndex(DATA_STREAM, indexToRemove)));

        DataStream ds = newState.metadata().dataStreams().get(DATA_STREAM);
        assertThat(ds.getIndices().size(), equalTo(2));
        assertThat(ds.getIndices().stream().map(i -> i.getName()).toList(), not(hasItem(indexToRemove)));
        // the removed index still exists as a standalone index and is no longer hidden
        assertNotNull(newState.metadata().index(indexToRemove));
        assertFalse(IndexMetadata.INDEX_HIDDEN_SETTING.get(newState.metadata().index(indexToRemove).getSettings()));
        // un-hiding changes settings, so the settings version must be bumped (IndexService.updateMetadata asserts this)
        assertThat(newState.metadata().index(indexToRemove).getSettingsVersion(), equalTo(beforeVersion + 1));
    }

    public void testRemoveWriteIndexFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of());
        String writeIndex = DataStream.getDefaultBackingIndexName(DATA_STREAM, 2);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.removeBackingIndex(DATA_STREAM, writeIndex)))
        );
        assertThat(e.getMessage(), containsString("is the write index"));
    }

    public void testRemoveIndexNotPartOfDataStreamFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.removeBackingIndex(DATA_STREAM, "standalone-index")))
        );
        assertThat(e.getMessage(), containsString("is not part of data stream"));
    }

    public void testAddToNonexistentDataStreamFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex("no-such-stream", "standalone-index")))
        );
        assertThat(e.getMessage(), containsString("data stream [no-such-stream] not found"));
    }

    public void testAddNonexistentIndexFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "no-such-index")))
        );
        assertThat(e.getMessage(), containsString("index [no-such-index] not found"));
    }

    public void testAddIndexWithAliasFails() {
        ClusterState base = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of());
        IndexMetadata aliased = IndexMetadata.builder("aliased-index")
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("my-alias"))
            .build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder(base.metadata()).put(aliased, false))
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, "aliased-index")))
        );
        assertThat(e.getMessage(), containsString("aliases"));
    }

    public void testAddIndexAlreadyBackingIndexFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 3)), List.of());
        // this index is already a backing index of the data stream
        String existingBackingIndex = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, existingBackingIndex)))
        );
        assertThat(e.getMessage(), containsString("already"));
    }

    public void testAddIndexBelongingToAnotherDataStreamFails() {
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(DATA_STREAM, 2), new Tuple<>("other-data-stream", 2)),
            List.of()
        );
        // this index is already a backing index of a different data stream
        String otherBackingIndex = DataStream.getDefaultBackingIndexName("other-data-stream", 1);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, otherBackingIndex)))
        );
        assertThat(e.getMessage(), containsString("is already a backing index of data stream [other-data-stream]"));
    }

    public void testAddIndexWithGenerationConflictNameFails() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of());
        // an index whose name matches the backing-index naming pattern but with a counter greater than the stream's
        // current generation would collide with a future rollover-created backing index; the request must be rejected with a
        // 400 (IllegalArgumentException) up front rather than failing the cluster state build with a 500 (IllegalStateException)
        String conflictingName = DataStream.getDefaultBackingIndexName(DATA_STREAM, 99);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> modify(cs, List.of(DataStreamAction.addBackingIndex(DATA_STREAM, conflictingName)))
        );
        assertThat(e.getMessage(), containsString("conflicts with a future backing index"));
    }

    public void testMultipleActionsAppliedAtomically() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 3)), List.of("standalone-index"));
        String indexToRemove = DataStream.getDefaultBackingIndexName(DATA_STREAM, 1);

        ClusterState newState = modify(
            cs,
            List.of(
                DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index"),
                DataStreamAction.removeBackingIndex(DATA_STREAM, indexToRemove)
            )
        );

        DataStream ds = newState.metadata().dataStreams().get(DATA_STREAM);
        // started with 3 backing indices, +1 added, -1 removed = 3
        assertThat(ds.getIndices().size(), equalTo(3));
        assertThat(ds.getIndices().stream().map(i -> i.getName()).toList(), hasItem("standalone-index"));
        assertThat(ds.getIndices().stream().map(i -> i.getName()).toList(), not(hasItem(indexToRemove)));
    }

    public void testFailedActionLeavesClusterStateUnchanged() {
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(DATA_STREAM, 2)), List.of("standalone-index"));
        String writeIndex = DataStream.getDefaultBackingIndexName(DATA_STREAM, 2);
        Metadata before = cs.metadata();

        // the second action targets the write index and must fail; the whole request fails with no partial state
        expectThrows(
            IllegalArgumentException.class,
            () -> modify(
                cs,
                List.of(
                    DataStreamAction.addBackingIndex(DATA_STREAM, "standalone-index"),
                    DataStreamAction.removeBackingIndex(DATA_STREAM, writeIndex)
                )
            )
        );
        // the input cluster state object is never mutated
        assertThat(cs.metadata().dataStreams().get(DATA_STREAM).getIndices().size(), equalTo(2));
        assertFalse(IndexMetadata.INDEX_HIDDEN_SETTING.get(before.index("standalone-index").getSettings()));
    }
}
