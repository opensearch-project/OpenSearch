/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.gateway.remote.ClusterMetadataManifest;

/**
 * A class encapsulating the cluster state manifest and its remote uploaded path
 */
public class RemoteClusterStateManifestInfo {

    private final ClusterMetadataManifest clusterMetadataManifest;
    private final String manifestFileName;

    public RemoteClusterStateManifestInfo(final ClusterMetadataManifest manifest, final String manifestFileName) {
        this.clusterMetadataManifest = manifest;
        this.manifestFileName = manifestFileName;
    }

    public ClusterMetadataManifest getClusterMetadataManifest() {
        return clusterMetadataManifest;
    }

    public String getManifestFileName() {
        return manifestFileName;
    }
}
