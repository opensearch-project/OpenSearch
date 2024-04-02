/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.IndicesService;

/**
 * Determines the {@link RemoteStorePathStrategy} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
public class RemoteStorePathStrategyResolver {

    private volatile PathType type;

    public RemoteStorePathStrategyResolver(ClusterSettings clusterSettings) {
        type = clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING);
        clusterSettings.addSettingsUpdateConsumer(IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING, this::setType);
    }

    public RemoteStorePathStrategy get() {
        return new RemoteStorePathStrategy(type, PathHashAlgorithm.FNV_1A);
    }

    private void setType(PathType type) {
        this.type = type;
    }
}
