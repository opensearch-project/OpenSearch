/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;

import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.opensearch.cluster.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MetadataDataStreamsServiceTests extends OpenSearchTestCase {

    private static final String DS = "logs-foo";

    /** A backing index with the @timestamp date mapping every data stream index requires. */
    private static IndexMetadata.Builder createBackingIndex(String dataStreamName, int generation) {
        try {
            return org.opensearch.cluster.DataStreamTestHelper.createBackingIndex(dataStreamName, generation)
                .putMapping(generateMapping("@timestamp"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** An arbitrary-named index carrying the given @timestamp mapping type ("date", "date_nanos", or "text"). */
    private static IndexMetadata.Builder arbitraryIndex(String name, String timestampType) {
        try {
            return IndexMetadata.builder(name)
                .settings(org.opensearch.common.settings.Settings.builder().put("index.version.created", org.opensearch.Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp", timestampType));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

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

    public void testAddArbitraryNamedIndexAsOldestBackingIndex() {
        // Migrating a pre-existing regular index into a data stream: the arbitrary-named index is attached as the
        // oldest backing index, ahead of the convention-following write index, and the generation is unchanged.
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 1).build();
        metadata.put(writeIndex, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(writeIndex.getIndex()), 1));
        metadata.put(arbitraryIndex("legacy-logs-2023", "date").build(), false);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.addBackingIndex(DS, "legacy-logs-2023"))
        );

        // Arbitrary-named index sorts first (oldest); the convention-following write index stays last.
        assertThat(backingIndexNames(updated), contains("legacy-logs-2023", DataStream.getDefaultBackingIndexName(DS, 1)));
        assertThat(updated.metadata().dataStreams().get(DS).getGeneration(), equalTo(1L));
        // The migrated index is marked hidden like every backing index.
        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(updated.metadata().index("legacy-logs-2023").getSettings()), equalTo(true));
    }

    public void testAddIndexWithoutTimestampMappingFails() {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 1).build();
        metadata.put(writeIndex, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(writeIndex.getIndex()), 1));
        // Candidate index maps @timestamp as text, not a date type.
        metadata.put(arbitraryIndex("legacy-logs-2023", "text").build(), false);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex(DS, "legacy-logs-2023")))
        );
        assertThat(e.getMessage(), containsString("does not have a [@timestamp] field mapped as a date type"));
    }

    public void testAddConventionNamedIndexWithoutTimestampMappingFails() {
        // A .ds-<name>-NNNNNN name is not proof the index came from create/rollover; a manually created index with the
        // convention name but no timestamp mapping must still be rejected.
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 2).build();
        metadata.put(writeIndex, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(writeIndex.getIndex()), 2));
        // Convention-named gen-1 index created manually with a non-date @timestamp mapping.
        metadata.put(arbitraryIndex(DataStream.getDefaultBackingIndexName(DS, 1), "text").build(), false);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                List.of(DataStreamAction.addBackingIndex(DS, DataStream.getDefaultBackingIndexName(DS, 1)))
            )
        );
        assertThat(e.getMessage(), containsString("does not have a [@timestamp] field mapped as a date type"));
    }

    public void testAddDateNanosTimestampMappingSucceeds() {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 1).build();
        metadata.put(writeIndex, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(writeIndex.getIndex()), 1));
        metadata.put(arbitraryIndex("legacy-logs-2023", "date_nanos").build(), false);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.addBackingIndex(DS, "legacy-logs-2023"))
        );
        assertThat(backingIndexNames(updated), contains("legacy-logs-2023", DataStream.getDefaultBackingIndexName(DS, 1)));
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

    public void testRemoveBackingIndexUnhidesIt() {
        // Backing indices are hidden; detaching one makes it visible again, mirroring the hide-on-attach behavior.
        ClusterState state = state(3, List.of(1, 2, 3));
        String toRemove = DataStream.getDefaultBackingIndexName(DS, 1);
        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(state.metadata().index(toRemove).getSettings()), equalTo(true));

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex(DS, toRemove))
        );

        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(updated.metadata().index(toRemove).getSettings()), equalTo(false));
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

    public void testRemoveIndexNotPartOfStreamFails() {
        // Removing an index that is not a backing index of the stream is rejected (remove only checks stream
        // membership, so the index need not exist elsewhere).
        ClusterState state = state(2, List.of(1, 2));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.removeBackingIndex(DS, "not-a-member")))
        );
        assertThat(e.getMessage(), containsString("is not part of data stream [" + DS + "]"));
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

    public void testAddMarksBackingIndexHidden() throws IOException {
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
            .putMapping(generateMapping("@timestamp"))
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

    /**
     * Builds a cluster state with two data streams. {@code logs-a} has backing gen 1 (arbitrary index {@code shared}
     * belongs to it), and {@code logs-b} has backing gen 1. {@code shared} is a standalone arbitrary-named index that
     * belongs to logs-a in the initial state.
     */
    private ClusterState twoStreamsWithSharedCandidate() {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata a1 = createBackingIndex("logs-a", 1).build();
        IndexMetadata b1 = createBackingIndex("logs-b", 1).build();
        IndexMetadata shared = arbitraryIndex("shared-idx", "date").build();
        metadata.put(a1, false);
        metadata.put(b1, false);
        metadata.put(shared, false);
        // logs-a owns both its convention write index and the arbitrary "shared-idx".
        metadata.put(new DataStream("logs-a", createTimestampField("@timestamp"), List.of(shared.getIndex(), a1.getIndex()), 1));
        metadata.put(new DataStream("logs-b", createTimestampField("@timestamp"), List.of(b1.getIndex()), 1));
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    private static List<String> backingIndexNames(ClusterState state, String dataStream) {
        return state.metadata().dataStreams().get(dataStream).getIndices().stream().map(Index::getName).collect(Collectors.toList());
    }

    public void testCannotAddSameIndexToTwoStreamsInOneRequest() {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata a1 = createBackingIndex("logs-a", 1).build();
        IndexMetadata b1 = createBackingIndex("logs-b", 1).build();
        metadata.put(a1, false);
        metadata.put(b1, false);
        metadata.put(new DataStream("logs-a", createTimestampField("@timestamp"), List.of(a1.getIndex()), 1));
        metadata.put(new DataStream("logs-b", createTimestampField("@timestamp"), List.of(b1.getIndex()), 1));
        metadata.put(arbitraryIndex("legacy-idx", "date").build(), false);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                List.of(DataStreamAction.addBackingIndex("logs-a", "legacy-idx"), DataStreamAction.addBackingIndex("logs-b", "legacy-idx"))
            )
        );
        assertThat(e.getMessage(), containsString("more than one data stream"));
    }

    public void testMoveBackingIndexBetweenStreamsRemoveThenAdd() {
        ClusterState state = twoStreamsWithSharedCandidate();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex("logs-a", "shared-idx"), DataStreamAction.addBackingIndex("logs-b", "shared-idx"))
        );

        assertThat(backingIndexNames(updated, "logs-a"), contains(DataStream.getDefaultBackingIndexName("logs-a", 1)));
        assertThat(backingIndexNames(updated, "logs-b"), contains("shared-idx", DataStream.getDefaultBackingIndexName("logs-b", 1)));
    }

    public void testMoveBackingIndexBetweenStreamsAddThenRemove() {
        // Reverse action order from the previous test; the outcome must be identical.
        ClusterState state = twoStreamsWithSharedCandidate();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.addBackingIndex("logs-b", "shared-idx"), DataStreamAction.removeBackingIndex("logs-a", "shared-idx"))
        );

        assertThat(backingIndexNames(updated, "logs-a"), contains(DataStream.getDefaultBackingIndexName("logs-a", 1)));
        assertThat(backingIndexNames(updated, "logs-b"), contains("shared-idx", DataStream.getDefaultBackingIndexName("logs-b", 1)));
    }

    public void testAddWithoutRemoveStillRejectedRegardlessOfOrder() {
        // Adding shared-idx to logs-b without removing it from logs-a must fail, in either action order.
        ClusterState state = twoStreamsWithSharedCandidate();

        IllegalArgumentException forward = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.addBackingIndex("logs-b", "shared-idx")))
        );
        assertThat(forward.getMessage(), containsString("more than one data stream"));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Timestamp-field mapping validation: every shape that leaves `type` null must be rejected.
    // ------------------------------------------------------------------------------------------------------------

    /** An index named {@code name} carrying the given raw mapping source, or no mapping at all when {@code null}. */
    private static IndexMetadata.Builder indexWithRawMapping(String name, String mappingSource) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(Settings.builder().put("index.version.created", Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0);
        if (mappingSource != null) {
            try {
                builder.putMapping(mappingSource);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return builder;
    }

    /** A stream {@code DS} at generation 1 (single convention-named write index) plus one standalone candidate index. */
    private ClusterState stateWithCandidate(IndexMetadata candidate) {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 1).build();
        metadata.put(writeIndex, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(writeIndex.getIndex()), 1));
        metadata.put(candidate, false);
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    private void expectTimestampMappingRejection(IndexMetadata candidate) {
        ClusterState state = stateWithCandidate(candidate);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                List.of(DataStreamAction.addBackingIndex(DS, candidate.getIndex().getName()))
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "index ["
                    + candidate.getIndex().getName()
                    + "] cannot be added as a backing index because it does not have a [@timestamp] field mapped as a date type"
            )
        );
    }

    public void testAddIndexWithNoMappingAtAllFails() {
        // mapping() is null: the whole property lookup is skipped and `type` stays null.
        IndexMetadata candidate = indexWithRawMapping("no-mapping-idx", null).build();
        assertThat(candidate.mapping(), nullValue());
        expectTimestampMappingRejection(candidate);
    }

    public void testAddIndexWhoseMappingHasNoPropertiesFails() {
        // A mapping exists but carries no "properties" block at all.
        IndexMetadata candidate = indexWithRawMapping("no-properties-idx", "{\"_meta\":{\"origin\":\"manual\"}}").build();
        assertThat(candidate.mapping(), notNullValue());
        expectTimestampMappingRejection(candidate);
    }

    public void testAddIndexWhosePropertiesIsNotAnObjectFails() {
        // "properties" is present but is a scalar, so the `properties instanceof Map` guard rejects it.
        IndexMetadata candidate = indexWithRawMapping("scalar-properties-idx", "{\"properties\":\"oops\"}").build();
        expectTimestampMappingRejection(candidate);
    }

    public void testAddIndexWhoseMappingLacksTimestampFieldFails() {
        // "properties" exists but has no @timestamp entry at all.
        IndexMetadata candidate = indexWithRawMapping("other-field-idx", "{\"properties\":{\"other\":{\"type\":\"date\"}}}").build();
        expectTimestampMappingRejection(candidate);
    }

    public void testAddIndexWhoseTimestampFieldIsNotAnObjectFails() {
        // @timestamp is present but is a scalar, so the `field instanceof Map` guard rejects it.
        IndexMetadata candidate = indexWithRawMapping("scalar-timestamp-idx", "{\"properties\":{\"@timestamp\":\"date\"}}").build();
        expectTimestampMappingRejection(candidate);
    }

    public void testAddIndexWhoseTimestampFieldHasNoTypeFails() {
        // @timestamp is an object but declares no "type", so `type` is null.
        IndexMetadata candidate = indexWithRawMapping("untyped-timestamp-idx", "{\"properties\":{\"@timestamp\":{\"index\":true}}}")
            .build();
        expectTimestampMappingRejection(candidate);
    }

    public void testValidateTimestampFieldMappingAcceptsBothDateTypes() throws IOException {
        // The accepting side of both halves of the `date`/`date_nanos` condition, exercised directly.
        MetadataDataStreamsService.validateTimestampFieldMapping(arbitraryIndex("d", "date").build(), "@timestamp");
        MetadataDataStreamsService.validateTimestampFieldMapping(arbitraryIndex("dn", "date_nanos").build(), "@timestamp");
        // A stream whose timestamp field is not the default name is validated against that name.
        IndexMetadata custom = IndexMetadata.builder("custom-ts")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("event.ingested", "date"))
            .build();
        MetadataDataStreamsService.validateTimestampFieldMapping(custom, "event.ingested");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.validateTimestampFieldMapping(custom, "@timestamp")
        );
        assertThat(e.getMessage(), containsString("does not have a [@timestamp] field mapped as a date type"));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Remove-backing-index guards.
    // ------------------------------------------------------------------------------------------------------------

    public void testRemoveLastBackingIndexFails() {
        // Size-1 check runs before the write-index check, so the message must be the "last backing index" one even
        // though the single index is also the write index.
        ClusterState state = state(1, List.of(1));
        String only = DataStream.getDefaultBackingIndexName(DS, 1);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(state, List.of(DataStreamAction.removeBackingIndex(DS, only)))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "cannot remove backing index ["
                    + only
                    + "] of data stream ["
                    + DS
                    + "] because it is the last backing index; delete the data stream instead"
            )
        );
    }

    public void testRemoveAlreadyVisibleBackingIndexLeavesSettingsUntouched() {
        // A backing index that is not hidden takes the false side of the unhide guard: no settings write at all.
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata writeIndex = createBackingIndex(DS, 1).build();
        IndexMetadata visible = IndexMetadata.builder("legacy-visible")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).put(IndexMetadata.SETTING_INDEX_HIDDEN, false))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        metadata.put(writeIndex, false);
        metadata.put(visible, false);
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(visible.getIndex(), writeIndex.getIndex()), 1));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        long settingsVersionBefore = state.metadata().index("legacy-visible").getSettingsVersion();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex(DS, "legacy-visible"))
        );

        assertThat(backingIndexNames(updated), contains(DataStream.getDefaultBackingIndexName(DS, 1)));
        assertThat(IndexMetadata.INDEX_HIDDEN_SETTING.get(updated.metadata().index("legacy-visible").getSettings()), equalTo(false));
        // The settings version is untouched, proving the unhide branch was skipped rather than re-applied.
        assertThat(updated.metadata().index("legacy-visible").getSettingsVersion(), equalTo(settingsVersionBefore));
    }

    public void testRemoveBackingIndexWithNoIndexMetadataSkipsUnhide() {
        // Defensive path: the stream references indices for which no IndexMetadata exists (Metadata skips data stream
        // lookup building entirely when there are no indices), so metadataBuilder.get(...) returns null.
        Index first = new Index(DataStream.getDefaultBackingIndexName(DS, 1), "uuid-1");
        Index writeIndex = new Index(DataStream.getDefaultBackingIndexName(DS, 2), "uuid-2");
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(new DataStream(DS, createTimestampField("@timestamp"), List.of(first, writeIndex), 2));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        assertThat(state.metadata().index(first.getName()), nullValue());

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.removeBackingIndex(DS, first.getName()))
        );

        assertThat(backingIndexNames(updated), contains(writeIndex.getName()));
        assertThat(updated.metadata().indices().size(), equalTo(0));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Shared-backing-index validation.
    // ------------------------------------------------------------------------------------------------------------

    public void testIndexNameListedTwiceWithinOneStreamIsNotReportedAsShared() {
        // The shared-index check keys on index name, so a stream that lists the same name twice would collide with
        // itself; the owner-equality half of the guard must let that through instead of failing the whole update.
        IndexMetadata a1 = createBackingIndex("logs-a", 1).build();
        IndexMetadata b1 = createBackingIndex("logs-b", 1).build();
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(a1, false);
        metadata.put(b1, false);
        // logs-a lists .ds-logs-a-000001 twice (same name, different UUIDs).
        metadata.put(
            new DataStream(
                "logs-a",
                createTimestampField("@timestamp"),
                List.of(new Index(a1.getIndex().getName(), "other-uuid"), a1.getIndex()),
                1
            )
        );
        metadata.put(new DataStream("logs-b", createTimestampField("@timestamp"), List.of(b1.getIndex()), 1));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        // Any action re-runs the full shared-index validation across every stream, including logs-a.
        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(DataStreamAction.addBackingIndex("logs-b", b1.getIndex().getName()))
        );

        assertThat(updated.metadata().dataStreams().get("logs-a").getIndices().size(), equalTo(2));
        assertThat(backingIndexNames(updated, "logs-b"), contains(b1.getIndex().getName()));
    }

    public void testSeveralActionsAcrossTwoStreamsComposeInOneUpdate() {
        // One request, four actions, two streams: each stream's intermediate result feeds the next action on it.
        IndexMetadata a1 = createBackingIndex("logs-a", 1).build();
        IndexMetadata a2 = createBackingIndex("logs-a", 2).build();
        IndexMetadata b1 = createBackingIndex("logs-b", 1).build();
        IndexMetadata shared = arbitraryIndex("roaming-idx", "date").build();
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(a1, false);
        metadata.put(a2, false);
        metadata.put(b1, false);
        metadata.put(shared, false);
        metadata.put(
            new DataStream("logs-a", createTimestampField("@timestamp"), List.of(shared.getIndex(), a1.getIndex(), a2.getIndex()), 2)
        );
        metadata.put(new DataStream("logs-b", createTimestampField("@timestamp"), List.of(b1.getIndex()), 1));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        ClusterState updated = MetadataDataStreamsService.modifyDataStream(
            state,
            List.of(
                DataStreamAction.removeBackingIndex("logs-a", "roaming-idx"),
                DataStreamAction.removeBackingIndex("logs-a", a1.getIndex().getName()),
                DataStreamAction.addBackingIndex("logs-b", "roaming-idx"),
                DataStreamAction.addBackingIndex("logs-b", a1.getIndex().getName())
            )
        );

        assertThat(backingIndexNames(updated, "logs-a"), contains(a2.getIndex().getName()));
        // The arbitrary name sorts first; .ds-logs-a-000001 is not a logs-b convention name so it also sorts first.
        assertThat(backingIndexNames(updated, "logs-b").size(), equalTo(3));
        assertThat(backingIndexNames(updated, "logs-b").get(2), equalTo(b1.getIndex().getName()));
        assertThat(updated.metadata().dataStreams().get("logs-b").getGeneration(), equalTo(1L));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Service entry points: request handling and the submitted cluster-manager task.
    // ------------------------------------------------------------------------------------------------------------

    /** Captures whichever ActionListener callback fires, so both success and failure can be asserted. */
    private static final class CapturingListener implements ActionListener<AcknowledgedResponse> {
        private AcknowledgedResponse response;
        private Exception failure;

        @Override
        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
            this.response = acknowledgedResponse;
        }

        @Override
        public void onFailure(Exception e) {
            this.failure = e;
        }
    }

    private ClusterManagerTaskThrottler.ThrottlingKey throttlingKey;

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        throttlingKey = mock(ClusterManagerTaskThrottler.ThrottlingKey.class);
        when(clusterService.registerClusterManagerTask(anyString(), anyBoolean())).thenReturn(throttlingKey);
        return clusterService;
    }

    @SuppressWarnings("unchecked")
    private AckedClusterStateUpdateTask<ClusterStateUpdateResponse> captureSubmittedTask(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(eq("update-data-streams"), captor.capture());
        return (AckedClusterStateUpdateTask<ClusterStateUpdateResponse>) captor.getValue();
    }

    public void testConstructorRegistersThrottledClusterManagerTask() {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        assertThat(service, notNullValue());
        verify(clusterService).registerClusterManagerTask("modify-data-stream", true);
    }

    public void testEmptyActionListIsAcknowledgedWithoutSubmittingAnyTask() {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        CapturingListener listener = new CapturingListener();

        service.modifyDataStream(
            new MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest(
                Collections.emptyList(),
                TimeValue.timeValueSeconds(30),
                TimeValue.timeValueSeconds(30)
            ),
            listener
        );

        assertThat(listener.failure, nullValue());
        assertThat(listener.response.isAcknowledged(), equalTo(true));
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
    }

    public void testSubmittedTaskCarriesRequestConfigurationAndAppliesActions() throws Exception {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        CapturingListener listener = new CapturingListener();
        String toRemove = DataStream.getDefaultBackingIndexName(DS, 1);

        service.modifyDataStream(
            new MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest(
                List.of(DataStreamAction.removeBackingIndex(DS, toRemove)),
                TimeValue.timeValueSeconds(11),
                TimeValue.timeValueSeconds(22)
            ),
            listener
        );

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = captureSubmittedTask(clusterService);
        assertThat(task.priority(), equalTo(Priority.HIGH));
        assertThat(task.timeout(), equalTo(TimeValue.timeValueSeconds(11)));
        assertThat(task.ackTimeout(), equalTo(TimeValue.timeValueSeconds(22)));
        assertThat(task.getClusterManagerThrottlingKey(), sameInstance(throttlingKey));

        // The task's execute() is the same metadata mutation the static helper performs.
        ClusterState updated = task.execute(state(2, List.of(1, 2)));
        assertThat(backingIndexNames(updated), contains(DataStream.getDefaultBackingIndexName(DS, 2)));

        // Nothing is reported to the caller until the state update is acked.
        assertThat(listener.response, nullValue());
        task.onAllNodesAcked(null);
        assertThat(listener.response.isAcknowledged(), equalTo(true));
    }

    public void testAckedWithExceptionAndAckTimeoutBothYieldUnacknowledged() {
        // Both non-happy acknowledgement paths map to an unacknowledged AcknowledgedResponse rather than a failure.
        ClusterService ackTimeoutClusterService = mockClusterService();
        CapturingListener ackTimeoutListener = new CapturingListener();
        new MetadataDataStreamsService(ackTimeoutClusterService).modifyDataStream(
            List.of(DataStreamAction.removeBackingIndex(DS, DataStream.getDefaultBackingIndexName(DS, 1))),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(5),
            ackTimeoutListener
        );
        captureSubmittedTask(ackTimeoutClusterService).onAckTimeout();
        assertThat(ackTimeoutListener.failure, nullValue());
        assertThat(ackTimeoutListener.response.isAcknowledged(), equalTo(false));

        ClusterService ackFailureClusterService = mockClusterService();
        CapturingListener ackFailureListener = new CapturingListener();
        new MetadataDataStreamsService(ackFailureClusterService).modifyDataStream(
            List.of(DataStreamAction.removeBackingIndex(DS, DataStream.getDefaultBackingIndexName(DS, 1))),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(5),
            ackFailureListener
        );
        captureSubmittedTask(ackFailureClusterService).onAllNodesAcked(new OpenSearchException("ack failed"));
        assertThat(ackFailureListener.failure, nullValue());
        assertThat(ackFailureListener.response.isAcknowledged(), equalTo(false));
    }

    public void testConvenienceOverloadPassesTimeoutsThrough() throws Exception {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        CapturingListener listener = new CapturingListener();

        service.modifyDataStream(
            List.of(DataStreamAction.addBackingIndex(DS, DataStream.getDefaultBackingIndexName(DS, 2))),
            TimeValue.timeValueSeconds(7),
            TimeValue.timeValueSeconds(13),
            listener
        );

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = captureSubmittedTask(clusterService);
        assertThat(task.timeout(), equalTo(TimeValue.timeValueSeconds(7)));
        assertThat(task.ackTimeout(), equalTo(TimeValue.timeValueSeconds(13)));
        // Idempotent add: the stream is unchanged but the task still runs.
        assertThat(backingIndexNames(task.execute(state(2, List.of(1, 2)))).size(), equalTo(2));
    }

    public void testTaskFailurePropagatesToTheCallersListener() {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        CapturingListener listener = new CapturingListener();

        service.modifyDataStream(
            List.of(DataStreamAction.removeBackingIndex(DS, "nope")),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(5),
            listener
        );

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = captureSubmittedTask(clusterService);
        // A rejected action surfaces from execute() and is reported through onFailure.
        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> task.execute(state(2, List.of(1, 2))));
        assertThat(thrown.getMessage(), containsString("is not part of data stream"));

        task.onFailure("update-data-streams", thrown);
        assertThat(listener.response, nullValue());
        assertThat(listener.failure, sameInstance(thrown));
    }

    public void testEmptyActionListPathDoesNotTouchTheClusterServiceBeyondRegistration() {
        ClusterService clusterService = mockClusterService();
        MetadataDataStreamsService service = new MetadataDataStreamsService(clusterService);
        verify(clusterService).registerClusterManagerTask(anyString(), anyBoolean());

        service.modifyDataStream(
            Collections.emptyList(),
            TimeValue.timeValueSeconds(1),
            TimeValue.timeValueSeconds(1),
            new CapturingListener()
        );

        verifyNoMoreInteractions(clusterService);
    }

    public void testUpdateRequestExposesItsActionsAndTimeouts() {
        List<DataStreamAction> actions = List.of(DataStreamAction.addBackingIndex(DS, "a"), DataStreamAction.removeBackingIndex(DS, "b"));
        MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest request =
            new MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest(
                actions,
                TimeValue.timeValueSeconds(3),
                TimeValue.timeValueSeconds(9)
            );

        assertThat(request.getActions(), equalTo(actions));
        assertThat(request.masterNodeTimeout(), equalTo(TimeValue.timeValueSeconds(3)));
        assertThat(request.ackTimeout(), equalTo(TimeValue.timeValueSeconds(9)));
    }
}
