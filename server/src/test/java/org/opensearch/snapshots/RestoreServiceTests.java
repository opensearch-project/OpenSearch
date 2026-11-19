/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestoreServiceTests extends OpenSearchTestCase {

    public void testUpdateDataStream() {
        String dataStreamName = "data-stream-1";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(backingIndexName))).thenReturn(indexMetadata);
        Index updatedIndex = new Index(backingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(updatedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest();

        DataStream updateDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(dataStreamName, updateDataStream.getName());
        assertEquals(Collections.singletonList(updatedIndex), updateDataStream.getIndices());
    }

    public void testUpdateDataStreamRename() {
        String dataStreamName = "data-stream-1";
        String renamedDataStreamName = "data-stream-2";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("data-stream-1").renameReplacement("data-stream-2");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }

    public void testPrefixNotChanged() {
        String dataStreamName = "ds-000001";
        String renamedDataStreamName = "ds2-000001";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String renamedBackingIndexName = DataStream.getDefaultBackingIndexName(renamedDataStreamName, 1);
        List<Index> indices = Collections.singletonList(new Index(backingIndexName, "uuid"));

        DataStream dataStream = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Metadata.Builder metadata = mock(Metadata.Builder.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.get(eq(renamedBackingIndexName))).thenReturn(indexMetadata);
        Index renamedIndex = new Index(renamedBackingIndexName, "uuid2");
        when(indexMetadata.getIndex()).thenReturn(renamedIndex);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest().renamePattern("ds-").renameReplacement("ds2-");

        DataStream renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());

        request = new RestoreSnapshotRequest().renamePattern("ds-000001").renameReplacement("ds2-000001");

        renamedDataStream = RestoreService.updateDataStream(dataStream, metadata, request);

        assertEquals(renamedDataStreamName, renamedDataStream.getName());
        assertEquals(Collections.singletonList(renamedIndex), renamedDataStream.getIndices());
    }

    public void testValidateReplicationTypeRestoreSettings_WhenSnapshotIsDocument_RestoreToDocument() {
        SnapshotId snapshotId = new SnapshotId("snapshotId", "123");
        Snapshot snapshot = new Snapshot("testRepo", snapshotId);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT.toString())
            .build();
        when(indexMetadata.getSettings()).thenReturn(settings);

        assertThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateReplicationTypeRestoreSettings(snapshot, ReplicationType.DOCUMENT.toString(), indexMetadata)
        );

    }

    public void testValidateReplicationTypeRestoreSettings_WhenSnapshotIsSegment_RestoreToDocument() {
        SnapshotId snapshotId = new SnapshotId("snapshotId", "123");
        Snapshot snapshot = new Snapshot("testRepo", snapshotId);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT.toString())
            .build();
        when(indexMetadata.getSettings()).thenReturn(settings);

        assertThrows(
            SnapshotRestoreException.class,
            () -> RestoreService.validateReplicationTypeRestoreSettings(snapshot, ReplicationType.SEGMENT.toString(), indexMetadata)
        );
    }

    public void testCleanupReportsEachRemovedRestoreOnce() {
        final List<RestoreInProgress.Entry> reported = new ArrayList<>();
        final RestoreService.CleanRestoreStateTaskExecutor executor = new RestoreService.CleanRestoreStateTaskExecutor(reported::add);
        final RestoreInProgress.Entry finished = restoreEntry("finished", RestoreInProgress.State.SUCCESS);
        final RestoreInProgress.Entry running = restoreEntry("running", RestoreInProgress.State.STARTED);
        final ClusterState before = stateWithRestores(finished, running);

        // Two tasks for one restore, as when several cluster state updates see it finished before the cleanup runs.
        final ClusterState after = executor.execute(
            before,
            List.of(
                new RestoreService.CleanRestoreStateTaskExecutor.Task("finished"),
                new RestoreService.CleanRestoreStateTaskExecutor.Task("finished")
            )
        ).resultingState;
        executor.clusterStatePublished(new ClusterChangedEvent("test", after, before));

        assertNull(RestoreService.restoreInProgress(after, "finished"));
        assertNotNull(RestoreService.restoreInProgress(after, "running"));
        assertEquals(List.of(finished), reported);
    }

    public void testCleanupReportsNothingWhenNoRestoreWasRemoved() {
        final List<RestoreInProgress.Entry> reported = new ArrayList<>();
        final RestoreService.CleanRestoreStateTaskExecutor executor = new RestoreService.CleanRestoreStateTaskExecutor(reported::add);
        final ClusterState before = stateWithRestores(
            restoreEntry("finished", RestoreInProgress.State.SUCCESS),
            restoreEntry("running", RestoreInProgress.State.STARTED)
        );
        final ClusterState after = ClusterState.builder(before).incrementVersion().build();

        executor.clusterStatePublished(new ClusterChangedEvent("test", after, before));

        assertTrue(reported.isEmpty());
    }

    public void testCleanupDoesNotReportAnUnfinishedRestore() {
        final List<RestoreInProgress.Entry> reported = new ArrayList<>();
        final RestoreService.CleanRestoreStateTaskExecutor executor = new RestoreService.CleanRestoreStateTaskExecutor(reported::add);
        final ClusterState before = stateWithRestores(restoreEntry("running", RestoreInProgress.State.STARTED));
        final ClusterState after = stateWithRestores();

        executor.clusterStatePublished(new ClusterChangedEvent("test", after, before));

        assertTrue(reported.isEmpty());
    }

    private static RestoreInProgress.Entry restoreEntry(String uuid, RestoreInProgress.State state) {
        final ShardId shardId = new ShardId(new Index("index-" + uuid, uuid + "-index-uuid"), 0);
        return new RestoreInProgress.Entry(
            uuid,
            new Snapshot("repo", new SnapshotId("snapshot", "snapshot-uuid")),
            state,
            List.of(shardId.getIndexName()),
            Map.of(shardId, new RestoreInProgress.ShardRestoreStatus("node", state.completed() ? state : RestoreInProgress.State.INIT))
        );
    }

    private static ClusterState stateWithRestores(RestoreInProgress.Entry... entries) {
        final RestoreInProgress.Builder restores = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : entries) {
            restores.add(entry);
        }
        return ClusterState.builder(ClusterName.DEFAULT).putCustom(RestoreInProgress.TYPE, restores.build()).build();
    }
}
