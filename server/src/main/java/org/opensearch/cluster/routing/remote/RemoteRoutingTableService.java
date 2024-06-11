/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for RemoteRoutingTableService. Exposes methods to orchestrate upload and download of routing table from remote store.
 */
public interface RemoteRoutingTableService extends LifecycleComponent {
    static final DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER =
        new DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable>() {
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

    DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(
        RoutingTable before,
        RoutingTable after
    );

    CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        BlobPath clusterBasePath
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    );

}
