/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.IndicesService;

import java.util.function.Supplier;

/**
 * Determines the {@link RemoteStorePathStrategy} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
public class RemoteStorePathStrategyResolver {

    private volatile PathType type;

    private volatile PathHashAlgorithm hashAlgorithm;

    private final Supplier<Version> minNodeVersionSupplier;

    public RemoteStorePathStrategyResolver(ClusterSettings clusterSettings, Supplier<Version> minNodeVersionSupplier) {
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        type = clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING);
        hashAlgorithm = clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING);
        clusterSettings.addSettingsUpdateConsumer(IndicesService.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING, this::setType);
        clusterSettings.addSettingsUpdateConsumer(IndicesService.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING, this::setHashAlgorithm);
    }

    public RemoteStorePathStrategy get() {
        PathType pathType;
        PathHashAlgorithm pathHashAlgorithm;
        // Min node version check ensures that we are enabling the new prefix type only when all the nodes understand it.
        pathType = Version.CURRENT.compareTo(minNodeVersionSupplier.get()) <= 0 ? type : PathType.FIXED;
        // If the path type is fixed, hash algorithm is not applicable.
        pathHashAlgorithm = pathType == PathType.FIXED ? null : hashAlgorithm;
        return new RemoteStorePathStrategy(pathType, pathHashAlgorithm);
    }

    private void setType(PathType type) {
        this.type = type;
    }

    private void setHashAlgorithm(PathHashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }
}
