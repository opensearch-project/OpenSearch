/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.gateway.remote.IndexMetadataManifest;

/**
 * A class encapsulating the cluster state manifest and its remote uploaded path
 */
public class RemoteIndexMetadataManifestInfo {

    private final IndexMetadataManifest indexMetadataManifest;
    private final String manifestVersion;

    public RemoteIndexMetadataManifestInfo(final IndexMetadataManifest indexMetadataManifest, final String manifestVersion) {
        this.indexMetadataManifest = indexMetadataManifest;
        this.manifestVersion = manifestVersion;
    }

    public IndexMetadataManifest getIndexMetadataManifest() {
        return indexMetadataManifest;
    }

    public String getManifestVersion() {
        return manifestVersion;
    }
}
