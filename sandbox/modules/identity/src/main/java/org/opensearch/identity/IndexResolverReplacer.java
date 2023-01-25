/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

public class IndexResolverReplacer {
//
//    private static final Set<String> NULL_SET = new HashSet<>(Collections.singleton(null));
//    private final Logger log = LogManager.getLogger(this.getClass());
//    private final IndexNameExpressionResolver resolver;
//    private final ClusterService clusterService;
//    private final ClusterInfoHolder clusterInfoHolder;
//    private volatile boolean respectRequestIndicesOptions = false;
//
//    public IndexResolverReplacer(IndexNameExpressionResolver resolver, ClusterService clusterService, ClusterInfoHolder clusterInfoHolder) {
//        this.resolver = resolver;
//        this.clusterService = clusterService;
//        this.clusterInfoHolder = clusterInfoHolder;
//    }
//
//    private static final boolean isAllWithNoRemote(final String... requestedPatterns) {
//
//        final List<String> patterns = requestedPatterns==null?null:Arrays.asList(requestedPatterns);
//
//        if(IndexNameExpressionResolver.isAllIndices(patterns)) {
//            return true;
//        }
//
//        if(patterns.size() == 1 && patterns.contains("*")) {
//            return true;
//        }
//
//        if(new HashSet<String>(patterns).equals(NULL_SET)) {
//            return true;
//        }
//
//        return false;
//    }
//
//    private static final boolean isLocalAll(String... requestedPatterns) {
//        return isLocalAll(requestedPatterns == null ? null : Arrays.asList(requestedPatterns));
//    }
//
//    private static final boolean isLocalAll(Collection<String> patterns) {
//        if(IndexNameExpressionResolver.isAllIndices(patterns)) {
//            return true;
//        }
//
//        if(patterns.contains("_all")) {
//            return true;
//        }
//
//        if(new HashSet<String>(patterns).equals(NULL_SET)) {
//            return true;
//        }
//
//        return false;
//    }
//
//    private class ResolvedIndicesProvider implements IndicesProvider {
//        private final Set<String> aliases;
//        private final Set<String> allIndices;
//        private final Set<String> originalRequested;
//
//        // set of previously resolved index requests to avoid resolving
//        // the same index more than once while processing bulk requests
//        private final Set<MultiKey> alreadyResolved;
//        private final String name;
//
//        ResolvedIndicesProvider(Object request) {
//            aliases = new HashSet<>();
//            allIndices = new HashSet<>();
//            originalRequested = new HashSet<>();
//            alreadyResolved = new HashSet<>();
//            name = request.getClass().getSimpleName();
//        }
//
//        private void resolveIndexPatterns(final String name, final IndicesOptions indicesOptions, final boolean enableCrossClusterResolution, final String[] original) {
//            final boolean isTraceEnabled = log.isTraceEnabled();
//            if (isTraceEnabled) {
//                log.trace("resolve requestedPatterns: "+ Arrays.toString(original));
//            }
//
//            if (isAllWithNoRemote(original)) {
//                if (isTraceEnabled) {
//                    log.trace(Arrays.toString(original) + " is an ALL pattern without any remote indices");
//                }
//                resolveToLocalAll();
//                return;
//            }
//
//            final List<String> localRequestedPatterns = new ArrayList<>(Arrays.asList(original));
//
//            final Collection<String> matchingAliases;
//            Collection<String> matchingAllIndices;
//            Collection<String> matchingDataStreams = null;
//
//            if (isLocalAll(original)) {
//                if (isTraceEnabled) {
//                    log.trace(Arrays.toString(original) + " is an LOCAL ALL pattern");
//                }
//                matchingAliases = Resolved.All_SET;
//                matchingAllIndices = Resolved.All_SET;
//
//            } else if (localRequestedPatterns.isEmpty()) {
//                if (isTraceEnabled) {
//                    log.trace(Arrays.toString(original) + " is a LOCAL EMPTY request");
//                }
//                matchingAllIndices = Collections.emptySet();
//                matchingAliases = Collections.emptySet();
//            }
//
//            else {
//                final ClusterState state = clusterService.state();
//                final Set<String> dateResolvedLocalRequestedPatterns = localRequestedPatterns
//                    .stream()
//                    .map(resolver::resolveDateMathExpression)
//                    .collect(Collectors.toSet());
//                final WildcardMatcher dateResolvedMatcher = WildcardMatcher.from(dateResolvedLocalRequestedPatterns);
//                //fill matchingAliases
//                final Map<String, IndexAbstraction> lookup = state.metadata().getIndicesLookup();
//                matchingAliases = lookup.entrySet()
//                    .stream()
//                    .filter(e -> e.getValue().getType() == ALIAS)
//                    .map(Map.Entry::getKey)
//                    .filter(dateResolvedMatcher)
//                    .collect(Collectors.toSet());
//
//                final boolean isDebugEnabled = log.isDebugEnabled();
//                try {
//                    matchingAllIndices = Arrays.asList(resolver.concreteIndexNames(state, indicesOptions, localRequestedPatterns.toArray(new String[0])));
//                    matchingDataStreams = resolver.dataStreamNames(state, indicesOptions, localRequestedPatterns.toArray(new String[0]));
//
//                    if (isDebugEnabled) {
//                        log.debug("Resolved pattern {} to indices: {} and data-streams: {}",
//                            localRequestedPatterns, matchingAllIndices, matchingDataStreams);
//                    }
//                } catch (IndexNotFoundException e1) {
//                    if (isDebugEnabled) {
//                        log.debug("No such indices for pattern {}, use raw value", localRequestedPatterns);
//                    }
//
//                    matchingAllIndices = dateResolvedLocalRequestedPatterns;
//                }
//            }
//
//            if (matchingDataStreams == null || matchingDataStreams.size() == 0) {
//                matchingDataStreams = Arrays.asList(NOOP);
//            }
//
//            if (isTraceEnabled) {
//                log.trace("Resolved patterns {} for {} ({}) to [aliases {}, allIndices {}, dataStreams {}, originalRequested{}]",
//                    original, name, this.name, matchingAliases, matchingAllIndices, matchingDataStreams, Arrays.toString(original));
//            }
//
//            resolveTo(matchingAliases, matchingAllIndices, matchingDataStreams, original);
//        }
//
//        private void resolveToLocalAll() {
//            aliases.add(Resolved.ANY);
//            allIndices.add(Resolved.ANY);
//            originalRequested.add(Resolved.ANY);
//        }
//
//        private void resolveTo(Iterable<String> matchingAliases, Iterable<String> matchingAllIndices,
//                               Iterable<String> matchingDataStreams, String[] original) {
//            aliases.addAll(StreamSupport.stream(matchingAliases.spliterator(), false).collect(Collectors.toList()));
//            allIndices.addAll(StreamSupport.stream(matchingAllIndices.spliterator(), false).collect(Collectors.toList()));
//            allIndices.addAll(StreamSupport.stream(matchingDataStreams.spliterator(), false).collect(Collectors.toList()));
//            originalRequested.addAll(Arrays.stream(original).collect(Collectors.toSet()));
//        }
//
//        @Override
//        public String[] provide(String[] original, Object localRequest, boolean supportsReplace) {
//            final IndicesOptions indicesOptions = indicesOptionsFrom(localRequest);
//            final boolean enableCrossClusterResolution = localRequest instanceof FieldCapabilitiesRequest
//                || localRequest instanceof SearchRequest
//                || localRequest instanceof ResolveIndexAction.Request;
//            // skip the whole thing if we have seen this exact resolveIndexPatterns request
//            if (alreadyResolved.add(new MultiKey(indicesOptions, enableCrossClusterResolution,
//                (original != null) ? new MultiKey(original, false) : null))) {
//                resolveIndexPatterns(localRequest.getClass().getSimpleName(), indicesOptions, enableCrossClusterResolution, original);
//            }
//            return IndicesProvider.NOOP;
//        }
//
//        Resolved resolved(IndicesOptions indicesOptions) {
//            final Resolved resolved = alreadyResolved.isEmpty() ? Resolved._LOCAL_ALL :
//                new Resolved(aliases, allIndices, originalRequested, indicesOptions);
//
//            if(log.isTraceEnabled()) {
//                log.trace("Finally resolved for {}: {}", name, resolved);
//            }
//
//            return resolved;
//        }
//    }
//
//    //dnfof
//    public boolean replace(final TransportRequest request, boolean retainMode, String... replacements) {
//        return getOrReplaceAllIndices(request, new IndicesProvider() {
//
//            @Override
//            public String[] provide(String[] original, Object request, boolean supportsReplace) {
//                if(supportsReplace) {
//                    if(retainMode && !isAllWithNoRemote(original)) {
//                        final Resolved resolved = resolveRequest(request);
//                        final List<String> retained = WildcardMatcher.from(resolved.getAllIndices()).getMatchAny(replacements, Collectors.toList());
//                        return retained.toArray(new String[0]);
//                    }
//                    return replacements;
//                } else {
//                    return NOOP;
//                }
//            }
//        }, false);
//    }
//
//    public Resolved resolveRequest(final Object request) {
//        if (log.isDebugEnabled()) {
//            log.debug("Resolve aliases, indices and types from {}", request.getClass().getSimpleName());
//        }
//
//        final ResolvedIndicesProvider resolvedIndicesProvider = new ResolvedIndicesProvider(request);
//
//        getOrReplaceAllIndices(request, resolvedIndicesProvider, false);
//
//        return resolvedIndicesProvider.resolved(indicesOptionsFrom(request));
//    }
//
//    public final static class Resolved {
//        private static final String ANY = "*";
//        private static final Set<String> All_SET = Set.of(ANY);
//        private static final Set<String> types = All_SET;
//        public static final Resolved _LOCAL_ALL = new Resolved(All_SET, All_SET, All_SET, Set.of(), SearchRequest.DEFAULT_INDICES_OPTIONS);
//
//        private final Set<String> aliases;
//        private final Set<String> allIndices;
//        private final Set<String> originalRequested;
//        private final boolean isLocalAll;
//        private final IndicesOptions indicesOptions;
//
//        public Resolved(final Set<String> aliases,
//                        final Set<String> allIndices,
//                        final Set<String> originalRequested,
//                        IndicesOptions indicesOptions) {
//            this.aliases = aliases;
//            this.allIndices = allIndices;
//            this.originalRequested = originalRequested;
//            this.isLocalAll = IndexResolverReplacer.isLocalAll(originalRequested.toArray(new String[0])) || (aliases.contains("*") && allIndices.contains("*"));
//            this.indicesOptions = indicesOptions;
//        }
//
//        public boolean isLocalAll() {
//            return isLocalAll;
//        }
//
//        public Set<String> getAliases() {
//            return aliases;
//        }
//
//        public Set<String> getAllIndices() {
//            return allIndices;
//        }
//
//        public Set<String> getAllIndicesResolved(ClusterService clusterService, IndexNameExpressionResolver resolver) {
//            if (isLocalAll) {
//                return new HashSet<>(Arrays.asList(resolver.concreteIndexNames(clusterService.state(), indicesOptions, "*")));
//            } else {
//                return allIndices;
//            }
//        }
//
//        public boolean isAllIndicesEmpty() {
//            return allIndices.isEmpty();
//        }
//
//        public Set<String> getTypes() {
//            return types;
//        }
//
//        @Override
//        public String toString() {
//            return "Resolved [aliases=" + aliases + ", allIndices=" + allIndices + ", types=" + types
//                + ", originalRequested=" + originalRequested + "]";
//        }
//
//        @Override
//        public int hashCode() {
//            final int prime = 31;
//            int result = 1;
//            result = prime * result + ((aliases == null) ? 0 : aliases.hashCode());
//            result = prime * result + ((allIndices == null) ? 0 : allIndices.hashCode());
//            result = prime * result + ((originalRequested == null) ? 0 : originalRequested.hashCode());
//            return result;
//        }
//
//        @Override
//        public boolean equals(Object obj) {
//            if (this == obj)
//                return true;
//            if (obj == null)
//                return false;
//            if (getClass() != obj.getClass())
//                return false;
//            Resolved other = (Resolved) obj;
//            if (aliases == null) {
//                if (other.aliases != null)
//                    return false;
//            } else if (!aliases.equals(other.aliases))
//                return false;
//            if (allIndices == null) {
//                if (other.allIndices != null)
//                    return false;
//            } else if (!allIndices.equals(other.allIndices))
//                return false;
//            if (originalRequested == null) {
//                if (other.originalRequested != null)
//                    return false;
//            } else if (!originalRequested.equals(other.originalRequested))
//                return false;
//            return true;
//        }
//    }
//
//    private List<String> renamedIndices(final RestoreSnapshotRequest request, final List<String> filteredIndices) {
//        try {
//            final List<String> renamedIndices = new ArrayList<>();
//            for (final String index : filteredIndices) {
//                String renamedIndex = index;
//                if (request.renameReplacement() != null && request.renamePattern() != null) {
//                    renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
//                }
//                renamedIndices.add(renamedIndex);
//            }
//            return renamedIndices;
//        } catch (PatternSyntaxException e) {
//            log.error("Unable to parse the regular expression denoted in 'rename_pattern'. Please correct the pattern an try again.");
//            throw e;
//        }
//    }
//
//
//    //--
//
//    @FunctionalInterface
//    public interface IndicesProvider {
//        public static final String[] NOOP = new String[0];
//        String[] provide(String[] original, Object request, boolean supportsReplace);
//    }
//
//    private boolean checkIndices(Object request, String[] indices, boolean needsToBeSizeOne, boolean allowEmpty) {
//
//        if(indices == IndicesProvider.NOOP) {
//            return false;
//        }
//
//        final boolean isTraceEnabled = log.isTraceEnabled();
//        if(!allowEmpty && (indices == null || indices.length == 0)) {
//            if(isTraceEnabled && request != null) {
//                log.trace("Null or empty indices for "+request.getClass().getName());
//            }
//            return false;
//        }
//
//        if(!allowEmpty && needsToBeSizeOne && indices.length != 1) {
//            if(isTraceEnabled && request != null) {
//                log.trace("To much indices for "+request.getClass().getName());
//            }
//            return false;
//        }
//
//        for (int i = 0; i < indices.length; i++) {
//            final String index = indices[i];
//            if(index == null || index.isEmpty()) {
//                //not allowed
//                if(isTraceEnabled && request != null) {
//                    log.trace("At least one null or empty index for "+request.getClass().getName());
//                }
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//    /**
//     * new
//     * @param request
//     * @param allowEmptyIndices
//     * @return
//     */
//    @SuppressWarnings("rawtypes")
//    private boolean getOrReplaceAllIndices(final Object request, final IndicesProvider provider, boolean allowEmptyIndices) {
//        final boolean isDebugEnabled = log.isDebugEnabled();
//        final boolean isTraceEnabled = log.isTraceEnabled();
//        if (isTraceEnabled) {
//            log.trace("getOrReplaceAllIndices() for "+request.getClass());
//        }
//
//        boolean result = true;
//
//        if (request instanceof BulkRequest) {
//
//            for (DocWriteRequest ar : ((BulkRequest) request).requests()) {
//                result = getOrReplaceAllIndices(ar, provider, false) && result;
//            }
//
//        } else if (request instanceof MultiGetRequest) {
//
//            for (ListIterator<Item> it = ((MultiGetRequest) request).getItems().listIterator(); it.hasNext();){
//                Item item = it.next();
//                result = getOrReplaceAllIndices(item, provider, false) && result;
//                /*if(item.index() == null || item.indices() == null || item.indices().length == 0) {
//                    it.remove();
//                }*/
//            }
//
//        } else if (request instanceof MultiSearchRequest) {
//
//            for (ListIterator<SearchRequest> it = ((MultiSearchRequest) request).requests().listIterator(); it.hasNext();) {
//                SearchRequest ar = it.next();
//                result = getOrReplaceAllIndices(ar, provider, false) && result;
//                /*if(ar.indices() == null || ar.indices().length == 0) {
//                    it.remove();
//                }*/
//            }
//
//        } else if (request instanceof MultiTermVectorsRequest) {
//
//            for (ActionRequest ar : (Iterable<TermVectorsRequest>) () -> ((MultiTermVectorsRequest) request).iterator()) {
//                result = getOrReplaceAllIndices(ar, provider, false) && result;
//            }
//
//        } else if(request instanceof PutMappingRequest) {
//            PutMappingRequest pmr = (PutMappingRequest) request;
//            Index concreteIndex = pmr.getConcreteIndex();
//            if(concreteIndex != null && (pmr.indices() == null || pmr.indices().length == 0)) {
//                String[] newIndices = provider.provide(new String[]{concreteIndex.getName()}, request, true);
//                if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                    return false;
//                }
//
//                ((PutMappingRequest) request).indices(newIndices);
//                ((PutMappingRequest) request).setConcreteIndex(null);
//            } else {
//                String[] newIndices = provider.provide(((PutMappingRequest) request).indices(), request, true);
//                if(checkIndices(request, newIndices, false, allowEmptyIndices) == false) {
//                    return false;
//                }
//                ((PutMappingRequest) request).indices(newIndices);
//            }
//        } else if(request instanceof RestoreSnapshotRequest) {
//
//            if(clusterInfoHolder.isLocalNodeElectedClusterManager() == Boolean.FALSE) {
//                return true;
//            }
//
//            final RestoreSnapshotRequest restoreRequest = (RestoreSnapshotRequest) request;
//            final SnapshotInfo snapshotInfo = SnapshotRestoreHelper.getSnapshotInfo(restoreRequest);
//
//            if (snapshotInfo == null) {
//                log.warn("snapshot repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found");
//                provider.provide(new String[]{"*"}, request, false);
//            } else {
//                final List<String> requestedResolvedIndices = SnapshotUtils.filterIndices(snapshotInfo.indices(), restoreRequest.indices(), restoreRequest.indicesOptions());
//                final List<String> renamedTargetIndices = renamedIndices(restoreRequest, requestedResolvedIndices);
//                //final Set<String> indices = new HashSet<>(requestedResolvedIndices);
//                //indices.addAll(renamedTargetIndices);
//                if (isDebugEnabled) {
//                    log.debug("snapshot: {} contains this indices: {}", snapshotInfo.snapshotId().getName(), renamedTargetIndices);
//                }
//                provider.provide(renamedTargetIndices.toArray(new String[0]), request, false);
//            }
//
//        } else if (request instanceof IndicesAliasesRequest) {
//            for(AliasActions ar: ((IndicesAliasesRequest) request).getAliasActions()) {
//                result = getOrReplaceAllIndices(ar, provider, false) && result;
//            }
//        } else if (request instanceof DeleteRequest) {
//            String[] newIndices = provider.provide(((DeleteRequest) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((DeleteRequest) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof UpdateRequest) {
//            String[] newIndices = provider.provide(((UpdateRequest) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((UpdateRequest) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof SingleShardRequest) {
//            final SingleShardRequest<?> singleShardRequest = (SingleShardRequest<?>) request;
//            final String index = singleShardRequest.index();
//            String[] indices = provider.provide(index == null ? null : new String[]{index}, request, true);
//            if (!checkIndices(request, indices, true, allowEmptyIndices)) {
//                return false;
//            }
//            singleShardRequest.index(indices.length != 1? null : indices[0]);
//        } else if (request instanceof FieldCapabilitiesIndexRequest) {
//            // FieldCapabilitiesIndexRequest does not support replacing the indexes.
//            // However, the indexes are always determined by FieldCapabilitiesRequest which will be reduced below
//            // (implements Replaceable). So IF an index arrives here, we can be sure that we have
//            // at least privileges for indices:data/read/field_caps
//            FieldCapabilitiesIndexRequest fieldCapabilitiesRequest = (FieldCapabilitiesIndexRequest) request;
//
//            String index = fieldCapabilitiesRequest.index();
//
//            String[] newIndices = provider.provide(new String[]{index}, request, true);
//            if (!checkIndices(request, newIndices, true, allowEmptyIndices)) {
//                return false;
//            }
//        } else if (request instanceof IndexRequest) {
//            String[] newIndices = provider.provide(((IndexRequest) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((IndexRequest) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof Replaceable) {
//            String[] newIndices = provider.provide(((Replaceable) request).indices(), request, true);
//            if(checkIndices(request, newIndices, false, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((Replaceable) request).indices(newIndices);
//        } else if (request instanceof BulkShardRequest) {
//            provider.provide(((ReplicationRequest) request).indices(), request, false);
//            //replace not supported?
//        } else if (request instanceof ReplicationRequest) {
//            String[] newIndices = provider.provide(((ReplicationRequest) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((ReplicationRequest) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof MultiGetRequest.Item) {
//            String[] newIndices = provider.provide(((MultiGetRequest.Item) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((MultiGetRequest.Item) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof CreateIndexRequest) {
//            String[] newIndices = provider.provide(((CreateIndexRequest) request).indices(), request, true);
//            if(checkIndices(request, newIndices, true, allowEmptyIndices) == false) {
//                return false;
//            }
//            ((CreateIndexRequest) request).index(newIndices.length!=1?null:newIndices[0]);
//        } else if (request instanceof CreateDataStreamAction.Request) {
//            provider.provide(((CreateDataStreamAction.Request) request).indices(), request, false);
//        } else if (request instanceof ReindexRequest) {
//            result = getOrReplaceAllIndices(((ReindexRequest) request).getDestination(), provider, false) && result;
//            result = getOrReplaceAllIndices(((ReindexRequest) request).getSearchRequest(), provider, false) && result;
//        } else if (request instanceof BaseNodesRequest) {
//            //do nothing
//        } else if (request instanceof MainRequest) {
//            //do nothing
//        } else if (request instanceof ClearScrollRequest) {
//            //do nothing
//        } else if (request instanceof SearchScrollRequest) {
//            //do nothing
//        } else if (request instanceof PutComponentTemplateAction.Request) {
//            // do nothing
//        } else {
//            if (isDebugEnabled) {
//                log.debug(request.getClass() + " not supported (It is likely not a indices related request)");
//            }
//            result = false;
//        }
//
//        return result;
//    }
//
//    private IndicesOptions indicesOptionsFrom(Object localRequest) {
//
//        if(!respectRequestIndicesOptions) {
//            return IndicesOptions.fromOptions(false, true, true, false, true);
//        }
//
//        if (IndicesRequest.class.isInstance(localRequest)) {
//            return ((IndicesRequest) localRequest).indicesOptions();
//        }
//        else if (RestoreSnapshotRequest.class.isInstance(localRequest)) {
//            return ((RestoreSnapshotRequest) localRequest).indicesOptions();
//        }
//        else {
//            return IndicesOptions.fromOptions(false, true, true, false, true);
//        }
//    }
//
//    @Subscribe
//    public void onDynamicConfigModelChanged(DynamicConfigModel dcm) {
//        respectRequestIndicesOptions = dcm.isRespectRequestIndicesEnabled();
//    }
}

