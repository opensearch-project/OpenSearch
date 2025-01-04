/*
 * Copyright 2015-2018 _floragunn_ GmbH
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.IndicesRequest.Replaceable;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesIndexRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetRequest.Item;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.main.MainRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.IndexUtils;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.plugin.systemindex.SystemIndexProtectionPlugin;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexAbstraction.Type.ALIAS;

public class IndexResolverReplacer {

    private static final Set<String> NULL_SET = new HashSet<>(Collections.singleton(null));
    private final Logger log = LogManager.getLogger(this.getClass());
    private final IndexNameExpressionResolver resolver;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final ClusterInfoHolder clusterInfoHolder;
    private volatile boolean respectRequestIndicesOptions = false;

    public IndexResolverReplacer(
        IndexNameExpressionResolver resolver,
        Supplier<ClusterState> clusterStateSupplier,
        ClusterInfoHolder clusterInfoHolder
    ) {
        this.resolver = resolver;
        this.clusterStateSupplier = clusterStateSupplier;
        this.clusterInfoHolder = clusterInfoHolder;
    }

    private static boolean isAllWithNoRemote(final String... requestedPatterns) {

        final List<String> patterns = requestedPatterns == null ? null : Arrays.asList(requestedPatterns);

        if (IndexNameExpressionResolver.isAllIndices(patterns)) {
            return true;
        }

        if (patterns.size() == 1 && patterns.contains("*")) {
            return true;
        }

        if (new HashSet<String>(patterns).equals(NULL_SET)) {
            return true;
        }

        return false;
    }

    private static boolean isLocalAll(String... requestedPatterns) {
        return isLocalAll(requestedPatterns == null ? null : Arrays.asList(requestedPatterns));
    }

    private static boolean isLocalAll(Collection<String> patterns) {
        if (IndexNameExpressionResolver.isAllIndices(patterns)) {
            return true;
        }

        if (patterns.contains("_all")) {
            return true;
        }

        if (new HashSet<String>(patterns).equals(NULL_SET)) {
            return true;
        }

        return false;
    }

    private class ResolvedIndicesProvider implements IndicesProvider {
        private final Set<String> aliases;
        private final Set<String> allIndices;
        private final Set<String> originalRequested;
        private final Set<String> remoteIndices;
        // set of previously resolved index requests to avoid resolving
        // the same index more than once while processing bulk requests
        private final Set<AlreadyResolvedKey> alreadyResolved;
        private final String name;

        private final class AlreadyResolvedKey {

            private final IndicesOptions indicesOptions;

            private final boolean enableCrossClusterResolution;

            private final String[] original;

            private AlreadyResolvedKey(final IndicesOptions indicesOptions, final boolean enableCrossClusterResolution) {
                this(indicesOptions, enableCrossClusterResolution, null);
            }

            private AlreadyResolvedKey(
                final IndicesOptions indicesOptions,
                final boolean enableCrossClusterResolution,
                final String[] original
            ) {
                this.indicesOptions = indicesOptions;
                this.enableCrossClusterResolution = enableCrossClusterResolution;
                this.original = original;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                AlreadyResolvedKey that = (AlreadyResolvedKey) o;
                return enableCrossClusterResolution == that.enableCrossClusterResolution
                    && Objects.equals(indicesOptions, that.indicesOptions)
                    && Arrays.equals(original, that.original);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(indicesOptions, enableCrossClusterResolution);
                result = 31 * result + Arrays.hashCode(original);
                return result;
            }
        }

        ResolvedIndicesProvider(Object request) {
            aliases = new HashSet<>();
            allIndices = new HashSet<>();
            originalRequested = new HashSet<>();
            remoteIndices = new HashSet<>();
            alreadyResolved = new HashSet<>();
            name = request.getClass().getSimpleName();
        }

        private void resolveIndexPatterns(
            final String name,
            final IndicesOptions indicesOptions,
            final boolean enableCrossClusterResolution,
            final String[] original
        ) {
            final boolean isTraceEnabled = log.isTraceEnabled();
            if (isTraceEnabled) {
                log.trace("resolve requestedPatterns: " + Arrays.toString(original));
            }

            if (isAllWithNoRemote(original)) {
                if (isTraceEnabled) {
                    log.trace(Arrays.toString(original) + " is an ALL pattern without any remote indices");
                }
                resolveToLocalAll();
                return;
            }

            Set<String> remoteIndices;
            final List<String> localRequestedPatterns = new ArrayList<>(Arrays.asList(original));

            final RemoteClusterService remoteClusterService = SystemIndexProtectionPlugin.GuiceHolder.getRemoteClusterService();

            if (remoteClusterService != null && remoteClusterService.isCrossClusterSearchEnabled() && enableCrossClusterResolution) {
                remoteIndices = new HashSet<>();
                final Map<String, OriginalIndices> remoteClusterIndices = SystemIndexProtectionPlugin.GuiceHolder.getRemoteClusterService()
                    .groupIndices(indicesOptions, original, idx -> resolver.hasIndexAbstraction(idx, clusterStateSupplier.get()));
                final Set<String> remoteClusters = remoteClusterIndices.keySet()
                    .stream()
                    .filter(k -> !RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY.equals(k))
                    .collect(Collectors.toSet());
                for (String remoteCluster : remoteClusters) {
                    for (String remoteIndex : remoteClusterIndices.get(remoteCluster).indices()) {
                        remoteIndices.add(RemoteClusterService.buildRemoteIndexName(remoteCluster, remoteIndex));
                    }
                }

                final Iterator<String> iterator = localRequestedPatterns.iterator();
                while (iterator.hasNext()) {
                    final String[] split = iterator.next().split(String.valueOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR), 2);
                    final WildcardMatcher matcher = WildcardMatcher.from(split[0]);
                    if (split.length > 1 && matcher.matchAny(remoteClusters)) {
                        iterator.remove();
                    }
                }

                if (isTraceEnabled) {
                    log.trace(
                        "CCS is enabled, we found this local patterns "
                            + localRequestedPatterns
                            + " and this remote patterns: "
                            + remoteIndices
                    );
                }

            } else {
                remoteIndices = Collections.emptySet();
            }

            final Collection<String> matchingAliases;
            Collection<String> matchingAllIndices;
            Collection<String> matchingDataStreams = null;

            if (isLocalAll(original)) {
                if (isTraceEnabled) {
                    log.trace(Arrays.toString(original) + " is an LOCAL ALL pattern");
                }
                matchingAliases = Resolved.All_SET;
                matchingAllIndices = Resolved.All_SET;

            } else if (!remoteIndices.isEmpty() && localRequestedPatterns.isEmpty()) {
                if (isTraceEnabled) {
                    log.trace(Arrays.toString(original) + " is an LOCAL EMPTY request");
                }
                matchingAllIndices = Collections.emptySet();
                matchingAliases = Collections.emptySet();
            }

            else {
                final ClusterState state = clusterStateSupplier.get();
                final Set<String> dateResolvedLocalRequestedPatterns = localRequestedPatterns.stream()
                    .map(resolver::resolveDateMathExpression)
                    .collect(Collectors.toSet());
                final WildcardMatcher dateResolvedMatcher = WildcardMatcher.from(dateResolvedLocalRequestedPatterns);
                // fill matchingAliases
                final Map<String, IndexAbstraction> lookup = state.metadata().getIndicesLookup();
                matchingAliases = lookup.entrySet()
                    .stream()
                    .filter(e -> e.getValue().getType() == ALIAS)
                    .map(Map.Entry::getKey)
                    .filter(dateResolvedMatcher)
                    .collect(Collectors.toSet());

                final boolean isDebugEnabled = log.isDebugEnabled();
                try {
                    matchingAllIndices = Arrays.asList(
                        resolver.concreteIndexNames(state, indicesOptions, localRequestedPatterns.toArray(new String[0]))
                    );
                    matchingDataStreams = resolver.dataStreamNames(state, indicesOptions, localRequestedPatterns.toArray(new String[0]));

                    if (isDebugEnabled) {
                        log.debug(
                            "Resolved pattern {} to indices: {} and data-streams: {}",
                            localRequestedPatterns,
                            matchingAllIndices,
                            matchingDataStreams
                        );
                    }
                } catch (IndexNotFoundException e1) {
                    if (isDebugEnabled) {
                        log.debug("No such indices for pattern {}, use raw value", localRequestedPatterns);
                    }

                    matchingAllIndices = dateResolvedLocalRequestedPatterns;
                }
            }

            if (matchingDataStreams == null || matchingDataStreams.size() == 0) {
                matchingDataStreams = Arrays.asList(NOOP);
            }

            if (isTraceEnabled) {
                log.trace(
                    "Resolved patterns {} for {} ({}) to [aliases {}, allIndices {}, dataStreams {}, originalRequested{}, remote indices {}]",
                    original,
                    name,
                    this.name,
                    matchingAliases,
                    matchingAllIndices,
                    matchingDataStreams,
                    Arrays.toString(original),
                    remoteIndices
                );
            }

            resolveTo(matchingAliases, matchingAllIndices, matchingDataStreams, original, remoteIndices);
        }

        private void resolveToLocalAll() {
            aliases.add(Resolved.ANY);
            allIndices.add(Resolved.ANY);
            originalRequested.add(Resolved.ANY);
        }

        private void resolveTo(
            Collection<String> matchingAliases,
            Collection<String> matchingAllIndices,
            Collection<String> matchingDataStreams,
            String[] original,
            Collection<String> remoteIndices
        ) {
            aliases.addAll(matchingAliases);
            allIndices.addAll(matchingAllIndices);
            allIndices.addAll(matchingDataStreams);
            originalRequested.addAll(Arrays.stream(original).collect(Collectors.toList()));
            this.remoteIndices.addAll(remoteIndices);
        }

        @Override
        public String[] provide(String[] original, Object localRequest, boolean supportsReplace) {
            final IndicesOptions indicesOptions = indicesOptionsFrom(localRequest);
            final boolean enableCrossClusterResolution = localRequest instanceof FieldCapabilitiesRequest
                || localRequest instanceof SearchRequest
                || localRequest instanceof ResolveIndexAction.Request;
            // skip the whole thing if we have seen this exact resolveIndexPatterns request
            final AlreadyResolvedKey alreadyResolvedKey;
            if (original != null) {
                alreadyResolvedKey = new AlreadyResolvedKey(indicesOptions, enableCrossClusterResolution, original);
            } else {
                alreadyResolvedKey = new AlreadyResolvedKey(indicesOptions, enableCrossClusterResolution);
            }
            if (alreadyResolved.add(alreadyResolvedKey)) {
                resolveIndexPatterns(localRequest.getClass().getSimpleName(), indicesOptions, enableCrossClusterResolution, original);
            }
            return IndicesProvider.NOOP;
        }

        Resolved resolved(IndicesOptions indicesOptions) {
            final Resolved resolved = alreadyResolved.isEmpty()
                ? Resolved._LOCAL_ALL
                : new Resolved(aliases, allIndices, originalRequested, remoteIndices, indicesOptions);

            if (log.isTraceEnabled()) {
                log.trace("Finally resolved for {}: {}", name, resolved);
            }

            return resolved;
        }
    }

    // dnfof
    public boolean replace(final TransportRequest request, boolean retainMode, String... replacements) {
        return getOrReplaceAllIndices(request, new IndicesProvider() {

            @Override
            public String[] provide(String[] original, Object request, boolean supportsReplace) {
                if (supportsReplace) {
                    if (retainMode && !isAllWithNoRemote(original)) {
                        final Resolved resolved = resolveRequest(request);
                        final List<String> retained = WildcardMatcher.from(resolved.getAllIndices())
                            .getMatchAny(replacements, Collectors.toList());
                        retained.addAll(resolved.getRemoteIndices());
                        return retained.toArray(new String[0]);
                    }
                    return replacements;
                } else {
                    return NOOP;
                }
            }
        }, false);
    }

    public boolean replace(final TransportRequest request, boolean retainMode, Collection<String> replacements) {
        return replace(request, retainMode, replacements.toArray(new String[replacements.size()]));
    }

    public Resolved resolveRequest(final Object request) {
        if (log.isDebugEnabled()) {
            log.debug("Resolve aliases, indices and types from {}", request.getClass().getSimpleName());
        }

        final ResolvedIndicesProvider resolvedIndicesProvider = new ResolvedIndicesProvider(request);

        getOrReplaceAllIndices(request, resolvedIndicesProvider, false);

        return resolvedIndicesProvider.resolved(indicesOptionsFrom(request));
    }

    public final static class Resolved {
        private static final String ANY = "*";
        private static final Set<String> All_SET = Set.of(ANY);
        private static final Set<String> types = All_SET;
        public static final Resolved _LOCAL_ALL = new Resolved(All_SET, All_SET, All_SET, Set.of(), SearchRequest.DEFAULT_INDICES_OPTIONS);

        private static final IndicesOptions EXACT_INDEX_OPTIONS = new IndicesOptions(
            EnumSet.of(IndicesOptions.Option.FORBID_ALIASES_TO_MULTIPLE_INDICES),
            EnumSet.noneOf(IndicesOptions.WildcardStates.class)
        );

        private final Set<String> aliases;
        private final Set<String> allIndices;
        private final Set<String> originalRequested;
        private final Set<String> remoteIndices;
        private final boolean isLocalAll;
        private final IndicesOptions indicesOptions;

        public Resolved(
            final Set<String> aliases,
            final Set<String> allIndices,
            final Set<String> originalRequested,
            final Set<String> remoteIndices,
            IndicesOptions indicesOptions
        ) {
            this.aliases = aliases;
            this.allIndices = allIndices;
            this.originalRequested = originalRequested;
            this.remoteIndices = remoteIndices;
            this.isLocalAll = IndexResolverReplacer.isLocalAll(originalRequested.toArray(new String[0]))
                || (aliases.contains("*") && allIndices.contains("*"));
            this.indicesOptions = indicesOptions;
        }

        public boolean isLocalAll() {
            return isLocalAll;
        }

        public Set<String> getAliases() {
            return aliases;
        }

        public Set<String> getAllIndices() {
            return allIndices;
        }

        public Set<String> getAllIndicesResolved(ClusterService clusterService, IndexNameExpressionResolver resolver) {
            return getAllIndicesResolved(clusterService::state, resolver);
        }

        public Set<String> getAllIndicesResolved(Supplier<ClusterState> clusterStateSupplier, IndexNameExpressionResolver resolver) {
            if (isLocalAll) {
                return new HashSet<>(Arrays.asList(resolver.concreteIndexNames(clusterStateSupplier.get(), indicesOptions, "*")));
            } else {
                return allIndices;
            }
        }

        public boolean isAllIndicesEmpty() {
            return allIndices.isEmpty();
        }

        public Set<String> getTypes() {
            return types;
        }

        public Set<String> getRemoteIndices() {
            return remoteIndices;
        }

        @Override
        public String toString() {
            return "Resolved [aliases="
                + aliases
                + ", allIndices="
                + allIndices
                + ", types="
                + types
                + ", originalRequested="
                + originalRequested
                + ", remoteIndices="
                + remoteIndices
                + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((aliases == null) ? 0 : aliases.hashCode());
            result = prime * result + ((allIndices == null) ? 0 : allIndices.hashCode());
            result = prime * result + ((originalRequested == null) ? 0 : originalRequested.hashCode());
            result = prime * result + ((remoteIndices == null) ? 0 : remoteIndices.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Resolved other = (Resolved) obj;
            if (aliases == null) {
                if (other.aliases != null) return false;
            } else if (!aliases.equals(other.aliases)) return false;
            if (allIndices == null) {
                if (other.allIndices != null) return false;
            } else if (!allIndices.equals(other.allIndices)) return false;
            if (originalRequested == null) {
                if (other.originalRequested != null) return false;
            } else if (!originalRequested.equals(other.originalRequested)) return false;
            if (remoteIndices == null) {
                if (other.remoteIndices != null) return false;
            } else if (!remoteIndices.equals(other.remoteIndices)) return false;
            return true;
        }

        public static Resolved ofIndex(String index) {
            Set<String> indexSet = Set.of(index);
            return new Resolved(Set.of(), indexSet, indexSet, Set.of(), EXACT_INDEX_OPTIONS);
        }
    }

    private List<String> renamedIndices(final RestoreSnapshotRequest request, final List<String> filteredIndices) {
        try {
            final List<String> renamedIndices = new ArrayList<>();
            for (final String index : filteredIndices) {
                String renamedIndex = index;
                if (request.renameReplacement() != null && request.renamePattern() != null) {
                    renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
                }
                renamedIndices.add(renamedIndex);
            }
            return renamedIndices;
        } catch (PatternSyntaxException e) {
            log.error("Unable to parse the regular expression denoted in 'rename_pattern'. Please correct the pattern an try again.");
            throw e;
        }
    }

    // --

    @FunctionalInterface
    public interface IndicesProvider {
        public static final String[] NOOP = new String[0];

        String[] provide(String[] original, Object request, boolean supportsReplace);
    }

    private boolean checkIndices(Object request, String[] indices, boolean needsToBeSizeOne, boolean allowEmpty) {

        if (indices == IndicesProvider.NOOP) {
            return false;
        }

        final boolean isTraceEnabled = log.isTraceEnabled();
        if (!allowEmpty && (indices == null || indices.length == 0)) {
            if (isTraceEnabled && request != null) {
                log.trace("Null or empty indices for " + request.getClass().getName());
            }
            return false;
        }

        if (!allowEmpty && needsToBeSizeOne && indices.length != 1) {
            if (isTraceEnabled && request != null) {
                log.trace("To much indices for " + request.getClass().getName());
            }
            return false;
        }

        for (int i = 0; i < indices.length; i++) {
            final String index = indices[i];
            if (index == null || index.isEmpty()) {
                // not allowed
                if (isTraceEnabled && request != null) {
                    log.trace("At least one null or empty index for " + request.getClass().getName());
                }
                return false;
            }
        }

        return true;
    }

    /**
     * new
     * @param request
     * @param allowEmptyIndices
     * @return
     */
    @SuppressWarnings("rawtypes")
    private boolean getOrReplaceAllIndices(final Object request, final IndicesProvider provider, boolean allowEmptyIndices) {
        final boolean isDebugEnabled = log.isDebugEnabled();
        final boolean isTraceEnabled = log.isTraceEnabled();
        if (isTraceEnabled) {
            log.trace("getOrReplaceAllIndices() for " + request.getClass());
        }

        boolean result = true;

        if (request instanceof BulkRequest) {

            for (DocWriteRequest ar : ((BulkRequest) request).requests()) {
                result = getOrReplaceAllIndices(ar, provider, false) && result;
            }

        } else if (request instanceof MultiGetRequest) {

            for (ListIterator<Item> it = ((MultiGetRequest) request).getItems().listIterator(); it.hasNext();) {
                Item item = it.next();
                result = getOrReplaceAllIndices(item, provider, false) && result;
                /*if(item.index() == null || item.indices() == null || item.indices().length == 0) {
                    it.remove();
                }*/
            }

        } else if (request instanceof MultiSearchRequest) {

            for (ListIterator<SearchRequest> it = ((MultiSearchRequest) request).requests().listIterator(); it.hasNext();) {
                SearchRequest ar = it.next();
                result = getOrReplaceAllIndices(ar, provider, false) && result;
                /*if(ar.indices() == null || ar.indices().length == 0) {
                    it.remove();
                }*/
            }

        } else if (request instanceof MultiTermVectorsRequest) {

            for (ActionRequest ar : (Iterable<TermVectorsRequest>) () -> ((MultiTermVectorsRequest) request).iterator()) {
                result = getOrReplaceAllIndices(ar, provider, false) && result;
            }

        } else if (request instanceof PutMappingRequest) {
            PutMappingRequest pmr = (PutMappingRequest) request;
            Index concreteIndex = pmr.getConcreteIndex();
            if (concreteIndex != null && (pmr.indices() == null || pmr.indices().length == 0)) {
                String[] newIndices = provider.provide(new String[] { concreteIndex.getName() }, request, true);
                if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                    return false;
                }

                ((PutMappingRequest) request).indices(newIndices);
                ((PutMappingRequest) request).setConcreteIndex(null);
            } else {
                String[] newIndices = provider.provide(((PutMappingRequest) request).indices(), request, true);
                if (checkIndices(request, newIndices, false, allowEmptyIndices) == false) {
                    return false;
                }
                ((PutMappingRequest) request).indices(newIndices);
            }
        } else if (request instanceof RestoreSnapshotRequest) {

            if (clusterInfoHolder.isLocalNodeElectedClusterManager() == Boolean.FALSE) {
                return true;
            }

            final RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
            final SnapshotInfo snapshotInfo = SnapshotRestoreHelper.getSnapshotInfo(restoreRequest);

            if (snapshotInfo == null) {
                log.warn(
                    "snapshot repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found"
                );
                provider.provide(new String[] { "*" }, request, false);
            } else {
                final List<String> requestedResolvedIndices = IndexUtils.filterIndices(
                    snapshotInfo.indices(),
                    restoreRequest.indices(),
                    restoreRequest.indicesOptions()
                );
                final List<String> renamedTargetIndices = renamedIndices(restoreRequest, requestedResolvedIndices);
                // final Set<String> indices = new HashSet<>(requestedResolvedIndices);
                // indices.addAll(renamedTargetIndices);
                if (isDebugEnabled) {
                    log.debug("snapshot: {} contains this indices: {}", snapshotInfo.snapshotId().getName(), renamedTargetIndices);
                }
                provider.provide(renamedTargetIndices.toArray(new String[0]), request, false);
            }

        } else if (request instanceof IndicesAliasesRequest) {
            for (AliasActions ar : ((IndicesAliasesRequest) request).getAliasActions()) {
                result = getOrReplaceAllIndices(ar, provider, false) && result;
            }
        } else if (request instanceof DeleteRequest) {
            String[] newIndices = provider.provide(((DeleteRequest) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((DeleteRequest) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof UpdateRequest) {
            String[] newIndices = provider.provide(((UpdateRequest) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((UpdateRequest) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof SingleShardRequest) {
            final SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;
            final String index = singleShardRequest.index();
            String[] indices = provider.provide(index == null ? null : new String[] { index }, request, true);
            if (!checkIndices(request, indices, true, allowEmptyIndices)) {
                return false;
            }
            singleShardRequest.index(indices.length != 1 ? null : indices[0]);
        } else if (request instanceof FieldCapabilitiesIndexRequest) {
            // FieldCapabilitiesIndexRequest does not support replacing the indexes.
            // However, the indexes are always determined by FieldCapabilitiesRequest which will be reduced below
            // (implements Replaceable). So IF an index arrives here, we can be sure that we have
            // at least privileges for indices:data/read/field_caps
            FieldCapabilitiesIndexRequest fieldCapabilitiesRequest = (FieldCapabilitiesIndexRequest) request;

            String index = fieldCapabilitiesRequest.index();

            String[] newIndices = provider.provide(new String[] { index }, request, true);
            if (!checkIndices(request, newIndices, true, allowEmptyIndices)) {
                return false;
            }
        } else if (request instanceof IndexRequest) {
            String[] newIndices = provider.provide(((IndexRequest) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((IndexRequest) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof Replaceable) {
            String[] newIndices = provider.provide(((Replaceable) request).indices(), request, true);
            if (checkIndices(request, newIndices, false, allowEmptyIndices) == false) {
                return false;
            }
            ((Replaceable) request).indices(newIndices);
        } else if (request instanceof BulkShardRequest) {
            provider.provide(((ReplicationRequest) request).indices(), request, false);
            // replace not supported?
        } else if (request instanceof ReplicationRequest) {
            String[] newIndices = provider.provide(((ReplicationRequest) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((ReplicationRequest) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof MultiGetRequest.Item) {
            String[] newIndices = provider.provide(((MultiGetRequest.Item) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((MultiGetRequest.Item) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof CreateIndexRequest) {
            String[] newIndices = provider.provide(((CreateIndexRequest) request).indices(), request, true);
            if (checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
                return false;
            }
            ((CreateIndexRequest) request).index(newIndices.length != 1 ? null : newIndices[0]);
        } else if (request instanceof ResizeRequest) {
            // clone or shrink operations
            provider.provide(((ResizeRequest) request).indices(), request, true);
            provider.provide(((ResizeRequest) request).getTargetIndexRequest().indices(), request, true);
        } else if (request instanceof CreateDataStreamAction.Request) {
            provider.provide(((CreateDataStreamAction.Request) request).indices(), request, false);
        } else if (request instanceof ReindexRequest) {
            result = getOrReplaceAllIndices(((ReindexRequest) request).getDestination(), provider, false) && result;
            result = getOrReplaceAllIndices(((ReindexRequest) request).getSearchRequest(), provider, false) && result;
        } else if (request instanceof BaseNodesRequest) {
            // do nothing
        } else if (request instanceof MainRequest) {
            // do nothing
        } else if (request instanceof ClearScrollRequest) {
            // do nothing
        } else if (request instanceof SearchScrollRequest) {
            // do nothing
        } else if (request instanceof PutComponentTemplateAction.Request) {
            // do nothing
        } else {
            if (isDebugEnabled) {
                log.debug(request.getClass() + " not supported (It is likely not a indices related request)");
            }
            result = false;
        }

        return result;
    }

    private IndicesOptions indicesOptionsFrom(Object localRequest) {
        if (IndicesRequest.class.isInstance(localRequest)) {
            return ((IndicesRequest) localRequest).indicesOptions();
        } else if (RestoreSnapshotRequest.class.isInstance(localRequest)) {
            return ((RestoreSnapshotRequest) localRequest).indicesOptions();
        } else {
            return IndicesOptions.fromOptions(false, true, true, false, true);
        }
    }
}
