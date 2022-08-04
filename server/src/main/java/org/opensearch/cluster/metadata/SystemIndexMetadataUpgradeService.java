/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;

/**
 * A service responsible for updating the metadata used by system indices.
 *
 * @opensearch.internal
 */
public class SystemIndexMetadataUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexMetadataUpgradeService.class);

    private final SystemIndices systemIndices;
    private final ClusterService clusterService;

    private boolean clusterManager = false;

    private volatile ImmutableOpenMap<String, IndexMetadata> lastIndexMetadataMap = ImmutableOpenMap.of();
    private volatile boolean updateTaskPending = false;

    public SystemIndexMetadataUpgradeService(SystemIndices systemIndices, ClusterService clusterService) {
        this.systemIndices = systemIndices;
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeClusterManager() != clusterManager) {
            this.clusterManager = event.localNodeClusterManager();
        }

        if (clusterManager && updateTaskPending == false) {
            final ImmutableOpenMap<String, IndexMetadata> indexMetadataMap = event.state().metadata().indices();

            if (lastIndexMetadataMap != indexMetadataMap) {
                for (ObjectObjectCursor<String, IndexMetadata> cursor : indexMetadataMap) {
                    if (cursor.value != lastIndexMetadataMap.get(cursor.key)) {
                        if (systemIndices.isSystemIndex(cursor.value.getIndex()) != cursor.value.isSystem()) {
                            updateTaskPending = true;
                            clusterService.submitStateUpdateTask(
                                "system_index_metadata_upgrade_service {system metadata change}",
                                new SystemIndexMetadataUpdateTask()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Task to update system index metadata.
     *
     * @opensearch.internal
     */
    public class SystemIndexMetadataUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            final ImmutableOpenMap<String, IndexMetadata> indexMetadataMap = currentState.metadata().indices();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();
            for (ObjectObjectCursor<String, IndexMetadata> cursor : indexMetadataMap) {
                if (cursor.value != lastIndexMetadataMap.get(cursor.key)) {
                    if (systemIndices.isSystemIndex(cursor.value.getIndex()) != cursor.value.isSystem()) {
                        updatedMetadata.add(IndexMetadata.builder(cursor.value).system(!cursor.value.isSystem()).build());
                    }
                }
            }

            if (updatedMetadata.isEmpty() == false) {
                final Metadata.Builder builder = Metadata.builder(currentState.metadata());
                updatedMetadata.forEach(idxMeta -> builder.put(idxMeta, true));
                return ClusterState.builder(currentState).metadata(builder).build();
            }
            return currentState;
        }

        @Override
        public void onFailure(String source, Exception e) {
            updateTaskPending = false;
            logger.error("failed to update system index metadata", e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            lastIndexMetadataMap = newState.metadata().indices();
            updateTaskPending = false;
        }
    }
}
