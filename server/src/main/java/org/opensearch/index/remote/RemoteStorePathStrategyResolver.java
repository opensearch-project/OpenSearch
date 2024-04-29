/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;

import java.util.function.Supplier;

/**
 * Determines the {@link RemoteStorePathStrategy} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteStorePathStrategyResolver {

    private final RemoteStoreSettings remoteStoreSettings;
    private final Supplier<Version> minNodeVersionSupplier;

    public RemoteStorePathStrategyResolver(RemoteStoreSettings remoteStoreSettings, Supplier<Version> minNodeVersionSupplier) {
        this.remoteStoreSettings = remoteStoreSettings;
        this.minNodeVersionSupplier = minNodeVersionSupplier;
    }

    public RemoteStorePathStrategy get() {
        PathType pathType;
        PathHashAlgorithm pathHashAlgorithm;
        // Min node version check ensures that we are enabling the new prefix type only when all the nodes understand it.
        pathType = Version.CURRENT.compareTo(minNodeVersionSupplier.get()) <= 0 ? remoteStoreSettings.getPathType() : PathType.FIXED;
        // If the path type is fixed, hash algorithm is not applicable.
        pathHashAlgorithm = pathType == PathType.FIXED ? null : remoteStoreSettings.getPathHashAlgorithm();
        return new RemoteStorePathStrategy(pathType, pathHashAlgorithm);
    }
}
