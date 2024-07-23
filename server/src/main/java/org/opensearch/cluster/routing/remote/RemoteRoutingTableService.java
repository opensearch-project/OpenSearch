/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.RoutingTableIncrementalDiff;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public interface RemoteRoutingTableService extends LifecycleComponent {

    public static final DiffableUtils.DiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER =
        new DiffableUtils.DiffableValueSerializer<String, IndexRoutingTable>() {
            @Override
            public IndexRoutingTable read(StreamInput in, String key) throws IOException {
                return IndexRoutingTable.readFrom(in);
            }

            @Override
            public void write(IndexRoutingTable value, StreamOutput out) throws IOException {
                value.writeTo(out);
            }

            @Override
            public Diff<IndexRoutingTable> readDiff(StreamInput in, String key) throws IOException {
                return IndexRoutingTable.readDiffFrom(in);
            }

            @Override
            public Diff<IndexRoutingTable> diff(IndexRoutingTable currentState, IndexRoutingTable previousState) {
                List<IndexShardRoutingTable> diffs = new ArrayList<>();
                for (Map.Entry<Integer, IndexShardRoutingTable> entry : currentState.getShards().entrySet()) {
                    Integer index = entry.getKey();
                    IndexShardRoutingTable currentShardRoutingTable = entry.getValue();
                    IndexShardRoutingTable previousShardRoutingTable = previousState.shard(index);
                    if (previousShardRoutingTable == null || !previousShardRoutingTable.equals(currentShardRoutingTable)) {
                        diffs.add(currentShardRoutingTable);
                    }
                }
                return new RoutingTableIncrementalDiff.IndexRoutingTableIncrementalDiff(diffs);
            }
        };

    List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable);

    void getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    );

    void getAsyncIndexRoutingTableDiffReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<RoutingTableIncrementalDiff> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    );

    DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    );

    void getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    );

    void getAsyncIndexRoutingDiffWriteAction(
        String clusterUUID,
        long term,
        long version,
        Map<String, Diff<IndexRoutingTable>> indexRoutingTableDiff,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    );

    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException;

}
