/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.indices.SystemIndices;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Excludes system indices from remote cluster state publication and restore. System index documents are stored on
 * local disk; publishing only their {@link IndexMetadata} to remote cluster state causes unrecoverable ghost indices
 * after nodes restart with empty local storage.
 *
 * @opensearch.internal
 */
class RemoteClusterStateIndexFilter {

    private final SystemIndices systemIndices;

    RemoteClusterStateIndexFilter(SystemIndices systemIndices) {
        this.systemIndices = systemIndices;
    }

    boolean includeIndex(String indexName) {
        return systemIndices.isSystemIndex(indexName) == false;
    }

    List<IndexMetadata> filterIndexMetadata(Collection<IndexMetadata> indices) {
        return indices.stream().filter(indexMetadata -> includeIndex(indexMetadata.getIndex().getName())).collect(Collectors.toList());
    }

    List<UploadedIndexMetadata> filterUploadedIndexMetadata(List<UploadedIndexMetadata> indices) {
        if (indices.isEmpty()) {
            return indices;
        }
        return indices.stream()
            .filter(uploadedIndexMetadata -> includeIndex(uploadedIndexMetadata.getIndexName()))
            .collect(Collectors.toList());
    }

    List<IndexRoutingTable> filterIndexRoutingTables(List<IndexRoutingTable> indexRoutingTables) {
        if (indexRoutingTables == null || indexRoutingTables.isEmpty()) {
            return indexRoutingTables;
        }
        return indexRoutingTables.stream()
            .filter(indexRoutingTable -> includeIndex(indexRoutingTable.getIndex().getName()))
            .collect(Collectors.toList());
    }

    void removeSystemIndicesFromUploadedMetadataMap(Map<String, UploadedIndexMetadata> uploadedIndexMetadataByName) {
        uploadedIndexMetadataByName.keySet().removeIf(indexName -> includeIndex(indexName) == false);
    }

}
