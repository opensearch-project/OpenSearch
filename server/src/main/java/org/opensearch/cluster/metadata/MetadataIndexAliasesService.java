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

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.AliasAction.NewAliasValidator;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.cluster.service.ClusterManagerTask.INDEX_ALIASES;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting add and remove aliases requests
 *
 * @opensearch.internal
 */
public class MetadataIndexAliasesService {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AliasValidator aliasValidator;

    private final MetadataDeleteIndexService deleteIndexService;

    private final NamedXContentRegistry xContentRegistry;
    private final ClusterManagerTaskThrottler.ThrottlingKey indexAliasTaskKey;

    @Inject
    public MetadataIndexAliasesService(
        ClusterService clusterService,
        IndicesService indicesService,
        AliasValidator aliasValidator,
        MetadataDeleteIndexService deleteIndexService,
        NamedXContentRegistry xContentRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.aliasValidator = aliasValidator;
        this.deleteIndexService = deleteIndexService;
        this.xContentRegistry = xContentRegistry;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        indexAliasTaskKey = clusterService.registerClusterManagerTask(INDEX_ALIASES, true);

    }

    public void indicesAliases(
        final IndicesAliasesClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "index-aliases",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return indexAliasTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyAliasActions(currentState, request.actions());
                }
            }
        );
    }

    /**
     * Handles the cluster state transition to a version that reflects the provided {@link AliasAction}s.
     */
    public ClusterState applyAliasActions(ClusterState currentState, Iterable<AliasAction> actions) {
        List<Index> indicesToClose = new ArrayList<>();
        Map<String, IndexService> indices = new HashMap<>();
        try {
            boolean changed = false;
            // Gather all the indexes that must be removed first so:
            // 1. We don't cause error when attempting to replace an index with a alias of the same name.
            // 2. We don't allow removal of aliases from indexes that we're just going to delete anyway. That'd be silly.
            Set<Index> indicesToDelete = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    IndexMetadata index = currentState.metadata().getIndices().get(action.getIndex());
                    if (index == null) {
                        throw new IndexNotFoundException(action.getIndex());
                    }
                    validateAliasTargetIsNotDSBackingIndex(currentState, action);
                    indicesToDelete.add(index.getIndex());
                    changed = true;
                }
            }
            // Remove the indexes if there are any to remove
            if (changed) {
                currentState = deleteIndexService.deleteIndices(currentState, indicesToDelete);
            }
            Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            // Run the remaining alias actions
            final Set<String> maybeModifiedIndices = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    // Handled above
                    continue;
                }
                IndexMetadata index = metadata.get(action.getIndex());
                if (index == null) {
                    throw new IndexNotFoundException(action.getIndex());
                }
                validateAliasTargetIsNotDSBackingIndex(currentState, action);
                NewAliasValidator newAliasValidator = (alias, indexRouting, filter, writeIndex) -> {
                    /* It is important that we look up the index using the metadata builder we are modifying so we can remove an
                     * index and replace it with an alias. */
                    Function<String, IndexMetadata> indexLookup = name -> metadata.get(name);
                    aliasValidator.validateAlias(alias, action.getIndex(), indexRouting, indexLookup);
                    if (Strings.hasLength(filter)) {
                        IndexService indexService = indices.get(index.getIndex().getName());
                        if (indexService == null) {
                            indexService = indicesService.indexService(index.getIndex());
                            if (indexService == null) {
                                // temporarily create the index and add mappings so we can parse the filter
                                try {
                                    indexService = indicesService.createIndex(index, emptyList(), false);
                                    indicesToClose.add(index.getIndex());
                                } catch (IOException e) {
                                    throw new OpenSearchException("Failed to create temporary index for parsing the alias", e);
                                }
                                indexService.mapperService().merge(index, MapperService.MergeReason.MAPPING_RECOVERY);
                            }
                            indices.put(action.getIndex(), indexService);
                        }
                        // the context is only used for validation so it's fine to pass fake values for the shard id,
                        // but the current timestamp should be set to real value as we may use `now` in a filtered alias
                        aliasValidator.validateAliasFilter(
                            alias,
                            filter,
                            indexService.newQueryShardContext(0, null, () -> System.currentTimeMillis(), null),
                            xContentRegistry
                        );
                    }
                };
                if (action.apply(newAliasValidator, metadata, index)) {
                    changed = true;
                    maybeModifiedIndices.add(index.getIndex().getName());
                }
            }

            for (final String maybeModifiedIndex : maybeModifiedIndices) {
                final IndexMetadata currentIndexMetadata = currentState.metadata().index(maybeModifiedIndex);
                final IndexMetadata newIndexMetadata = metadata.get(maybeModifiedIndex);
                // only increment the aliases version if the aliases actually changed for this index
                if (currentIndexMetadata.getAliases().equals(newIndexMetadata.getAliases()) == false) {
                    assert currentIndexMetadata.getAliasesVersion() == newIndexMetadata.getAliasesVersion();
                    metadata.put(new IndexMetadata.Builder(newIndexMetadata).aliasesVersion(1 + currentIndexMetadata.getAliasesVersion()));
                }
            }

            if (changed) {
                ClusterState updatedState = ClusterState.builder(currentState).metadata(metadata).build();
                // even though changes happened, they resulted in 0 actual changes to metadata
                // i.e. remove and add the same alias to the same index
                if (!updatedState.metadata().equalsAliases(currentState.metadata())) {
                    return updatedState;
                }
            }
            return currentState;
        } finally {
            for (Index index : indicesToClose) {
                indicesService.removeIndex(index, NO_LONGER_ASSIGNED, "created for alias processing");
            }
        }
    }

    private void validateAliasTargetIsNotDSBackingIndex(ClusterState currentState, AliasAction action) {
        IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(action.getIndex());
        assert indexAbstraction != null : "invalid cluster metadata. index [" + action.getIndex() + "] was not found";
        if (indexAbstraction.getParentDataStream() != null) {
            throw new IllegalArgumentException(
                "The provided index [ "
                    + action.getIndex()
                    + "] is a backing index belonging to data stream ["
                    + indexAbstraction.getParentDataStream().getName()
                    + "]. Data streams and their backing indices don't support alias operations."
            );
        }
    }
}
