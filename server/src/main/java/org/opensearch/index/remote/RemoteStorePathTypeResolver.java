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
public class RemoteStorePathTypeResolver {

    private volatile RemoteStorePathType type;

    public RemoteStorePathTypeResolver(ClusterSettings clusterSettings) {
        type = clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING);
        clusterSettings.addSettingsUpdateConsumer(IndicesService.CLUSTER_REMOTE_STORE_PATH_PREFIX_TYPE_SETTING, this::setType);
    }

    public RemoteStorePathType getType() {
        return type;
    }

    public void setType(RemoteStorePathType type) {
        this.type = type;
    }
}
