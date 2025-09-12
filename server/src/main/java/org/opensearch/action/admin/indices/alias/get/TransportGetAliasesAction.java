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

package org.opensearch.action.admin.indices.alias.get;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.indices.SystemIndices;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for listing index aliases
 *
 * @opensearch.internal
 */
public class TransportGetAliasesAction extends TransportClusterManagerNodeReadAction<GetAliasesRequest, GetAliasesResponse>
    implements
        TransportIndicesResolvingAction<GetAliasesRequest> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportGetAliasesAction.class);

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetAliasesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetAliasesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAliasesRequest::new,
            indexNameExpressionResolver,
            true
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected String executor() {
        // very lightweight operation all in memory no need to fork to a thread pool
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        // Resolve with system index access since we're just checking blocks
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request)
            );
    }

    @Override
    protected GetAliasesResponse read(StreamInput in) throws IOException {
        return new GetAliasesResponse(in);
    }

    @Override
    protected void clusterManagerOperation(GetAliasesRequest request, ClusterState state, ActionListener<GetAliasesResponse> listener) {
        String[] concreteIndices;
        // Switch to a context which will drop any deprecation warnings, because there may be indices resolved here which are not
        // returned in the final response. We'll add warnings back later if necessary in checkSystemIndexAccess.
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().newStoredContext(false)) {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        }
        final boolean systemIndexAccessAllowed = indexNameExpressionResolver.isSystemIndexAccessAllowed();
        final Map<String, List<AliasMetadata>> aliases = state.metadata().findAliases(request, concreteIndices);
        listener.onResponse(
            new GetAliasesResponse(postProcess(request, concreteIndices, aliases, state, systemIndexAccessAllowed, systemIndices))
        );
    }

    /**
     * Fills alias result with empty entries for requested indices when no specific aliases were requested.
     */
    static Map<String, List<AliasMetadata>> postProcess(
        GetAliasesRequest request,
        String[] concreteIndices,
        final Map<String, List<AliasMetadata>> aliases,
        ClusterState state,
        boolean systemIndexAccessAllowed,
        SystemIndices systemIndices
    ) {
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        final Map<String, List<AliasMetadata>> mapBuilder = new HashMap<>(aliases);
        for (String index : concreteIndices) {
            if (aliases.get(index) == null && noAliasesSpecified) {
                List<AliasMetadata> previous = mapBuilder.put(index, Collections.emptyList());
                assert previous == null;
            }
        }
        final Map<String, List<AliasMetadata>> finalResponse = Collections.unmodifiableMap(mapBuilder);
        if (systemIndexAccessAllowed == false) {
            checkSystemIndexAccess(request, systemIndices, state, finalResponse);
        }
        return finalResponse;
    }

    private static void checkSystemIndexAccess(
        GetAliasesRequest request,
        SystemIndices systemIndices,
        ClusterState state,
        final Map<String, List<AliasMetadata>> aliasesMap
    ) {
        Set<String> systemIndicesNames = new HashSet<>();
        for (Iterator<String> it = aliasesMap.keySet().iterator(); it.hasNext();) {
            String indexName = it.next();
            IndexMetadata index = state.metadata().index(indexName);
            if (index != null && index.isSystem()) {
                systemIndicesNames.add(indexName);
            }
        }
        if (systemIndicesNames.isEmpty() == false) {
            systemIndicesNames.forEach(
                systemIndexName -> deprecationLogger.deprecate(
                    "open_system_index_access_" + systemIndexName,
                    "this request accesses system indices: [{}], but in a future major version, direct access to system "
                        + "indices will be prevented by default",
                    systemIndexName
                )
            );
        } else {
            checkSystemAliasAccess(request, systemIndices);
        }
    }

    private static void checkSystemAliasAccess(GetAliasesRequest request, SystemIndices systemIndices) {
        final List<String> systemAliases = Arrays.stream(request.aliases())
            .filter(alias -> systemIndices.isSystemIndex(alias))
            .collect(Collectors.toList());
        if (systemAliases.isEmpty() == false) {
            deprecationLogger.deprecate(
                "open_system_alias_access",
                "this request accesses aliases with names reserved for system indices: {}, but in a future major version, direct"
                    + "access to system indices and their aliases will not be allowed",
                systemAliases
            );
        }
    }

    @Override
    public ResolvedIndices resolveIndices(GetAliasesRequest request) {
        ClusterState state = this.clusterService.state();

        // The index resolution object in this method is advanced, even though it might not look like it in
        // the clusterManagerOperation() method on the first glance.
        //
        // GetAliasesRequest can be in several different states:
        // - no aliases and no indices specified: both the aliases and the indices attribute in GetAliasesRequest are
        // empty arrays. This will then cause all aliases and all indices to be resolved and referenced.
        // - an alias and no indices: the indices attribute in GetAliasesRequest will be an empty array, which will be
        // resolved by indexNameExpressionResolver.concreteIndexNames() to all indices. The action will then filter
        // all indices to those that are member of the specified alias
        // - no aliases and one or more indices: the aliases attribute in GetAliasesRequest will be an empty array,
        // which will be resolved by state.metadata().findAliases() to all aliases, but limited to the aliases
        // containing one of the specified indices
        // - both aliases and indices specified: this is then the intersection
        //
        // For both indices and aliases, patterns can be specified. The indicesOptions of the request only apply to the indices.
        //
        // The consequence for the resolveIndices() method is that the semantics of the return value might be debatable.
        // We resort to have just the index names on the top level, because it is the most precise dimension.
        // Alias names are included as a sub action named "indices:admin/aliases/get[aliases]"

        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        String[] concreteIndices;

        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().newStoredContext(false)) {
            concreteIndices = indexNameExpressionResolver.concreteResolvedIndices(state, request)
                .withoutResolutionErrors()
                .namesOfConcreteIndicesAsArray();
        }
        Map<String, List<AliasMetadata>> indexToAliasesMap = state.metadata().findAliases(request, concreteIndices);

        // If no aliases were specified in the request, we will report all indices matching the indices expression
        // If there were aliases specified, we will report just the indices that are members of the aliases
        // That follows the logic in the postProcess() method above.
        Collection<String> indices = noAliasesSpecified ? Arrays.asList(concreteIndices) : indexToAliasesMap.keySet();

        return ResolvedIndices.of(indices)
            .withLocalSubActions(
                GetAliasesAction.NAME + "[aliases]",
                ResolvedIndices.Local.of(
                    indexToAliasesMap.values().stream().flatMap(Collection::stream).map(AliasMetadata::alias).collect(Collectors.toSet())
                )
            );
    }
}
