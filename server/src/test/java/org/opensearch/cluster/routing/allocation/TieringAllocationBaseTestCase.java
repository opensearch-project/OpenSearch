/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;

import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;

@SuppressForbidden(reason = "feature flag overrides")
public abstract class TieringAllocationBaseTestCase extends RemoteShardsBalancerBaseTestCase {

    public ClusterState updateIndexMetadataForTiering(
        ClusterState clusterState,
        int localIndices,
        int remoteIndices,
        String tieringState,
        boolean isWarmIndex
    ) {
        Metadata.Builder mb = Metadata.builder(clusterState.metadata());
        for (int i = 0; i < localIndices; i++) {
            IndexMetadata indexMetadata = clusterState.metadata().index(getIndexName(i, false));
            Settings settings = indexMetadata.getSettings();
            mb.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(settings)
                            .put(settings)
                            .put(INDEX_TIERING_STATE.getKey(), tieringState)
                            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), isWarmIndex)
                    )
            );
        }
        for (int i = 0; i < remoteIndices; i++) {
            IndexMetadata indexMetadata = clusterState.metadata().index(getIndexName(i, true));
            Settings settings = indexMetadata.getSettings();
            mb.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(settings)
                            .put(settings)
                            .put(INDEX_TIERING_STATE.getKey(), tieringState)
                            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), isWarmIndex)
                    )
            );
        }
        Metadata metadata = mb.build();
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }
}
