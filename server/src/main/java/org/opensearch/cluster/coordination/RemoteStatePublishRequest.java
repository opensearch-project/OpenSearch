/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.ClusterState;
import org.opensearch.gateway.remote.ClusterMetadataManifest;

import java.util.Objects;

/**
 * PublishRequest created by downloading the accepted {@link ClusterState} from Remote Store, using  the published {@link ClusterMetadataManifest}
 *
 * @opensearch.internal
 */
public class RemoteStatePublishRequest extends PublishRequest {
    private final ClusterMetadataManifest manifest;

    public RemoteStatePublishRequest(ClusterState acceptedState, ClusterMetadataManifest acceptedManifest) {
        super(acceptedState);
        this.manifest = acceptedManifest;
    }

    public ClusterMetadataManifest getAcceptedManifest() {
        return manifest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RemoteStatePublishRequest that = (RemoteStatePublishRequest) o;
        return Objects.equals(manifest, that.manifest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), manifest);
    }

    @Override
    public String toString() {
        return "RemoteStatePublishRequest{" + super.toString() + "manifest=" + manifest + "} ";
    }
}
