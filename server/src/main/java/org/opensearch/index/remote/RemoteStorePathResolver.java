/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.indices.IndicesService;

/**
 * Determines the {@link RemoteStorePathType} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
public class RemoteStorePathResolver {

    private final ClusterSettings clusterSettings;

    public RemoteStorePathResolver(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public RemoteStorePathType resolveType() {
        return clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING);
    }
}
