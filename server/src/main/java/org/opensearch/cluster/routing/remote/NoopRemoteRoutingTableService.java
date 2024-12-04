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
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.RoutingTableIncrementalDiff;
import org.opensearch.cluster.routing.StringKeyDiffProvider;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.List;

/**
 * Noop impl for RemoteRoutingTableService.
 */
public class NoopRemoteRoutingTableService extends AbstractLifecycleComponent implements RemoteRoutingTableService {

    @Override
    public List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable) {
        return List.of();
    }

    @Override
    public StringKeyDiffProvider<IndexRoutingTable> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return new RoutingTableIncrementalDiff(RoutingTable.builder().build(), RoutingTable.builder().build());
    }

    @Override
    public void getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        // noop
    }

    @Override
    public void getAsyncIndexRoutingDiffWriteAction(
        String clusterUUID,
        long term,
        long version,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        // noop
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    ) {
        // noop
        return List.of();
    }

    @Override
    public void getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    ) {
        // noop
    }

    @Override
    public void getAsyncIndexRoutingTableDiffReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<Diff<RoutingTable>> latchedActionListener
    ) {
        // noop
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    ) {
        // noop
        return List.of();
    }

    @Override
    protected void doStart() {
        // noop
    }

    @Override
    protected void doStop() {
        // noop
    }

    @Override
    protected void doClose() throws IOException {
        // noop
    }

    @Override
    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException {
        // noop
    }

    public void deleteStaleIndexRoutingDiffPaths(List<String> stalePaths) throws IOException {
        // noop
    }
}
