/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public interface RemoteRoutingTableService extends LifecycleComponent {
    DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER =
        new DiffableUtils.NonDiffableValueSerializer<>() {
            @Override
            public void write(IndexRoutingTable value, StreamOutput out) throws IOException {
                value.writeTo(out);
            }

            @Override
            public IndexRoutingTable read(StreamInput in, String key) throws IOException {
                return IndexRoutingTable.readFrom(in);
            }
        };

    List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable);

    CheckedRunnable<IOException> getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    );

    DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    );

    CheckedRunnable<IOException> getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    );

    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException;

}
