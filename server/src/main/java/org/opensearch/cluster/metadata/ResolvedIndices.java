/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.ActionType;
import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;
import org.opensearch.transport.RemoteClusterService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class that encapsulates resolved indices. Resolved indices do not any wildcards or date math expressions.
 * However, in contrast to the concept of "concrete indices", resolved indices might not exist yet, or might
 * refer to aliases or data streams.
 * <p>
 * ResolvedIndices classes are primarily created by the resolveIndices() method in TransportIndicesResolvingAction.
 * <p>
 * Instances of ResolvedIndices are immutable. It will be not possible to modify the returned collection instances.
 * All methods which add/modify elements will return a new ResolvedIndices instance.
 * <p>
 * How resolved indices are obtained depends on the respective action and the associated requests:
 * <ul>
 *     <li>If a request carries an index expression (i.e, might contain patterns or date math expressions), the index
 *     expression must be resolved using the appropriate index options; these might be request-specific or action-specific.</li>
 *     <li>Some requests already carry concrete indices; in these cases, the names of the concrete indices can be
 *     just taken without further evaluation</li>
 * </ul>
 */
@ExperimentalApi
public class ResolvedIndices extends OptionallyResolvedIndices {

    /**
     * Creates a ResolvedIndices object with a ResolvedIndices.Local object that returns the given indicesAliasesAndDataStreams
     * as names().
     *
     * @param indicesAliasesAndDataStreams An array of strings. The strings must be not null. No further validation will be performed.
     */
    public static ResolvedIndices of(String... indicesAliasesAndDataStreams) {
        return new ResolvedIndices(new Local(Set.of(indicesAliasesAndDataStreams), null, Map.of()), Remote.EMPTY);
    }

    /**
     * Creates a ResolvedIndices object with a ResolvedIndices.Local.Concrete object that returns the given indices
     * by its concreteIndices() and namesOfConcreteIndices() method. The names() method will return the same names as
     * namesOfConcreteIndices().
     */
    public static ResolvedIndices of(Index... indices) {
        return new ResolvedIndices(
            new Local.Concrete(
                Set.of(indices),
                Stream.of(indices).map(Index::getName).collect(Collectors.toUnmodifiableSet()),
                null,
                Map.of(),
                List.of()
            ),
            Remote.EMPTY
        );
    }

    /**
     * Creates a ResolvedIndices object with a ResolvedIndices.Local object that returns the given indicesAliasesAndDataStreams
     * as names().
     *
     * @param indicesAliasesAndDataStreams A collection of strings. The strings must be not null. No further validation will be performed.
     */
    public static ResolvedIndices of(Collection<String> indicesAliasesAndDataStreams) {
        return new ResolvedIndices(new Local(Set.copyOf(indicesAliasesAndDataStreams), null, Map.of()), Remote.EMPTY);
    }

    /**
     * Creates a ResolvedIndices object where local() returns the given Local object.
     */
    public static ResolvedIndices of(Local local) {
        return new ResolvedIndices(local, Remote.EMPTY);
    }

    /**
     * Returns an OptionallyResolvedIndices object which is not a ResolvedIndices object. This represents the case
     * that indices cannot be determined.
     */
    public static OptionallyResolvedIndices unknown() {
        return OptionallyResolvedIndices.unknown();
    }

    /**
     * Creates a ResolvedIndices object with a ResolvedIndices.Local object that returns the given indicesAliasesAndDataStreams
     * as names().
     *
     * @param indicesAliasesAndDataStreams An array of strings. Any string that is null will be not included in this object. No further validation will be performed.
     */
    public static ResolvedIndices ofNonNull(String... indicesAliasesAndDataStreams) {
        Set<String> indexSet = new HashSet<>(indicesAliasesAndDataStreams.length);

        for (String index : indicesAliasesAndDataStreams) {
            if (index != null) {
                indexSet.add(index);
            }
        }

        return new ResolvedIndices(new Local(Collections.unmodifiableSet(indexSet), null, Map.of()), Remote.EMPTY);
    }

    private final Local local;
    private final Remote remote;

    private ResolvedIndices(Local local, Remote remote) {
        this.local = local;
        this.remote = remote;
    }

    @Override
    public Local local() {
        return this.local;
    }

    public Remote remote() {
        return this.remote;
    }

    /**
     * Creates a new copy of this ResolvedIndices object which has the given remote indices associated.
     */
    public ResolvedIndices withRemoteIndices(Map<String, OriginalIndices> remoteIndices) {
        if (remoteIndices.isEmpty()) {
            return this;
        }

        Map<String, OriginalIndices> newRemoteIndices = new HashMap<>(remoteIndices);
        newRemoteIndices.putAll(this.remote.clusterToOriginalIndicesMap);

        return new ResolvedIndices(this.local, new Remote(Collections.unmodifiableMap(newRemoteIndices)));
    }

    /**
     * Returns a ResolvedIndices object associated with the given OriginalIndices object for the local part. This is only for
     * convenience, no semantics are implied.
     */
    public ResolvedIndices withLocalOriginalIndices(OriginalIndices originalIndices) {
        return new ResolvedIndices(this.local.withOriginalIndices(originalIndices), this.remote);
    }

    /**
     * Creates a new copy of this ResolvedIndices object which has the given ResolvedIndices.Local instance associated
     * by the given actionType.
     */
    public ResolvedIndices withLocalSubActions(ActionType<?> actionType, ResolvedIndices.Local local) {
        return new ResolvedIndices(this.local.withSubActions(actionType, local), this.remote);
    }

    /**
     * Creates a new copy of this ResolvedIndices object which has the given ResolvedIndices.Local instance associated
     * by the given actionType.
     */
    public ResolvedIndices withLocalSubActions(String actionType, ResolvedIndices.Local local) {
        return new ResolvedIndices(this.local.withSubActions(actionType, local), this.remote);
    }

    /**
     * Returns true if both the local and the remote indices do not refer to any names (existing or non-existing).
     */
    public boolean isEmpty() {
        return this.local.isEmpty() && this.remote.isEmpty();
    }

    @Override
    public String toString() {
        return "ResolvedIndices{" + "local=" + local + ", remote=" + remote + '}';
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ResolvedIndices otherResolvedIndices)) {
            return false;
        }

        return this.local.equals(otherResolvedIndices.local) && this.remote.equals(otherResolvedIndices.remote);
    }

    @Override
    public int hashCode() {
        return this.local.hashCode() + this.remote.hashCode() * 31;
    }

    /**
     * Represents the local (i.e., non-remote) indices referenced by the respective request.
     */
    @ExperimentalApi
    public static class Local extends OptionallyResolvedIndices.Local {
        protected final Set<String> names;
        protected final OriginalIndices originalIndices;
        protected final Map<String, Local> subActions;
        private Set<String> namesOfIndices;

        /**
         * Creates a ResolvedIndices.Local object that returns the given indicesAliasesAndDataStreams as names().
         *
         * @param indicesAliasesAndDataStreams A collection of strings. The strings must be not null. No further validation will be performed.
         */
        public static Local of(Collection<String> indicesAliasesAndDataStreams) {
            return new Local(Set.copyOf(indicesAliasesAndDataStreams), null, Map.of());
        }

        /**
         * Creates a ResolvedIndices.Local object that returns the given indicesAliasesAndDataStreams as names().
         *
         * @param indicesAliasesAndDataStreams An array of strings. The strings must be not null. No further validation will be performed.
         */
        public static Local of(String... indicesAliasesAndDataStreams) {
            return of(Arrays.asList(indicesAliasesAndDataStreams));
        }

        /**
         * Creates a new instance.
         * <p>
         * Note: The caller of this method must make sure that the passed objects are immutable.
         * For this reason, this constructor is private. This contract is guaranteed by the static
         * constructor methods in this file.
         */
        private Local(Set<String> names, OriginalIndices originalIndices, Map<String, Local> subActions) {
            this.names = names;
            this.originalIndices = originalIndices;
            this.subActions = subActions;
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         *
         * @return an unmodifiable set of names of indices, aliases and/or data streams.
         */
        public Set<String> names() {
            return this.names;
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         *
         * @return an array of names of indices, aliases and/or data streams.
         */
        public String[] namesAsArray() {
            return this.names().toArray(new String[0]);
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         * <p>
         * In case this is an isUnknown() object, this will return a set of all concrete indices on the cluster (incl.
         * hidden and closed indices). This might be a large object. Be prepared to handle such a large object.
         * <p>
         * <strong>This method will be only rarely needed. In most cases <code>ResolvedIndices.names()</code> will be sufficient.</strong>
         */
        @Override
        public Set<String> names(ClusterState clusterState) {
            return this.names;
        }

        /**
         * Returns all the local names. Any data streams or aliases will be replaced by the member index names.
         * In contrast to namesOfConcreteIndices(), this will keep the names of non-existing indices and will never
         * throw an exception.
         *
         * @return an unmodifiable set of index names
         */
        public Set<String> namesOfIndices(ClusterState clusterState) {
            Set<String> result = this.namesOfIndices;

            if (result == null) {
                Map<String, IndexAbstraction> indicesLookup = clusterState.metadata().getIndicesLookup();
                result = new HashSet<>(this.names.size());
                for (String name : this.names) {
                    IndexAbstraction indexAbstraction = indicesLookup.get(name);

                    if (indexAbstraction == null) {
                        // We keep the names of non existing indices
                        result.add(name);
                    } else if (indexAbstraction instanceof IndexAbstraction.Index) {
                        // For normal indices, we just keep its name
                        result.add(name);
                    } else {
                        // This is an alias or data stream
                        for (IndexMetadata index : indexAbstraction.getIndices()) {
                            result.add(index.getIndex().getName());
                        }
                    }
                }

                result = Collections.unmodifiableSet(result);

                this.namesOfIndices = result;
            }

            return result;
        }

        /**
         * Returns any OriginalIndices object associated with this object.
         * Note: This is just a convenience method for code that passes around ResolvedIndices objects for managing
         * information. This object will be only present if you add it to the object.
         */
        public OriginalIndices originalIndices() {
            return this.originalIndices;
        }

        /**
         * Sub-actions can be used to specify indices which play a different role in the action processing.
         * For example, the swiss-army-knife IndicesAliases action can delete indices. The subActions() property
         * can be used to specify indices with such special roles.
         */
        public Map<String, Local> subActions() {
            return this.subActions;
        }

        /**
         * Returns true if there are no local indices.
         */
        @Override
        public boolean isEmpty() {
            return this.names.isEmpty();
        }

        /**
         * Returns true if the local names contain an entry with the given name.
         */
        @Override
        public boolean contains(String name) {
            return this.names.contains(name);
        }

        /**
         * Returns true if the local names contain any of the specified names.
         */
        @Override
        public boolean containsAny(Collection<String> names) {
            return names.stream().anyMatch(this.names::contains);
        }

        /**
         * Returns true if any of the local names match the given predicate.
         */
        @Override
        public boolean containsAny(Predicate<String> namePredicate) {
            return this.names.stream().anyMatch(namePredicate);
        }

        /**
         * Returns a ResolvedIndices.Local object associated with the given OriginalIndices object. This is only for
         * convenience, no semantics are implied.
         */
        public ResolvedIndices.Local withOriginalIndices(OriginalIndices originalIndices) {
            return new Local(this.names, originalIndices, this.subActions);
        }

        public ResolvedIndices.Local withSubActions(String key, ResolvedIndices.Local local) {
            Map<String, Local> subActions = new HashMap<>(this.subActions);
            subActions.put(key, local);
            return new Local(this.names, this.originalIndices, Collections.unmodifiableMap(subActions));
        }

        public ResolvedIndices.Local withSubActions(ActionType<?> actionType, ResolvedIndices.Local local) {
            return this.withSubActions(actionType.name(), local);
        }

        @Override
        public String toString() {
            if (this.subActions.isEmpty()) {
                return "{names=" + names() + "}";
            } else {
                return "{names=" + names() + ", subActions=" + subActions + '}';
            }
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResolvedIndices.Local otherLocal)) {
                return false;
            }

            return this.names.equals(otherLocal.names) && this.subActions.equals(otherLocal.subActions);
        }

        @Override
        public int hashCode() {
            return this.names.hashCode() + this.subActions.hashCode() * 31;
        }

        /**
         * This is a specialization of the {@link ResolvedIndices.Local} class which additionally
         * carries {@link Index} objects. The {@link IndexNameExpressionResolver} produces such objects.
         * <p>
         * <strong>Important:</strong> The methods that give access to the concrete indices can throw
         * exceptions such as {@link org.opensearch.index.IndexNotFoundException} if the concrete indices
         * could not be determined. The methods from the Local super class, such as names(), still give
         * access to index information.
         */
        @ExperimentalApi
        public static class Concrete extends Local {
            private static final Concrete EMPTY = new Concrete(Set.of(), Set.of(), null, Map.of(), List.of());

            public static Concrete empty() {
                return EMPTY;
            }

            public static Concrete of(Index... concreteIndices) {
                return new Concrete(
                    Set.of(concreteIndices),
                    Stream.of(concreteIndices).map(Index::getName).collect(Collectors.toSet()),
                    null,
                    Map.of(),
                    List.of()
                );
            }

            /**
             * Creates a new RemoteIndices.Local.Concrete object with the given concrete indices and names.
             * This is primarily used by IndexNameExpressionResolver to construct return values, that's why it is
             * package private. There may be more names than concrete indices, for example when a referenced index does
             * not exist. Thus, names should be usually a super set of concreteIndices. This method does not verify
             * that, it is the duty of the caller to make this sure.
             */
            static Concrete of(Set<Index> concreteIndices, Set<String> names) {
                return new Concrete(Set.copyOf(concreteIndices), Set.copyOf(names), null, Map.of(), List.of());
            }

            private final Set<Index> concreteIndices;
            private final List<RuntimeException> resolutionErrors;

            private Concrete(
                Set<Index> concreteIndices,
                Set<String> names,
                OriginalIndices originalIndices,
                Map<String, Local> subActions,
                List<RuntimeException> resolutionErrors
            ) {
                super(names, originalIndices, subActions);
                this.concreteIndices = concreteIndices;
                this.resolutionErrors = resolutionErrors;
            }

            /**
             * Returns the concrete indices. This might throw an exception if there were issues during index resolution.
             * <p>
             * If you need access to index information while avoiding exceptions, you can use the following approaches:
             * <ul>
             *     <li>Use the names() method instead. This might also contain of non-existing indices or invalid names</li>
             *     <li>Use withoutResolutionErrors().concreteIndices(). This will only contain existing indices.</li>
             * </ul>
             *
             * @throws org.opensearch.index.IndexNotFoundException This exception is thrown for several conditions:
             * 1. If one of the index expression pointed to a missing index, alias or data stream and the IndicesOptions
             * used during resolution do not allow such a case. 2. If the set of resolved concrete indices is empty and
             * the IndicesOptions used during resolution do not allow such a case.
             * @throws IllegalArgumentException if one of the aliases resolved to multiple indices and
             * IndicesOptions.FORBID_ALIASES_TO_MULTIPLE_INDICES was set.
             */
            public Set<Index> concreteIndices() {
                checkResolutionErrors();
                return this.concreteIndices;
            }

            /**
             * Returns the concrete indices. This might throw an exception if there were issues during index resolution.
             * <p>
             * If you need access to index information while avoiding exceptions, you can use the following approaches:
             * <ul>
             *     <li>Use the names() method instead. This might also contain of non-existing indices or invalid names</li>
             *     <li>Use withoutResolutionErrors().concreteIndices(). This will only contain existing indices.</li>
             * </ul>
             *
             * @throws org.opensearch.index.IndexNotFoundException This exception is thrown for several conditions:
             * 1. If one of the index expression pointed to a missing index, alias or data stream and the IndicesOptions
             * used during resolution do not allow such a case. 2. If the set of resolved concrete indices is empty and
             * the IndicesOptions used during resolution do not allow such a case.
             * @throws IllegalArgumentException if one of the aliases resolved to multiple indices and
             * IndicesOptions.FORBID_ALIASES_TO_MULTIPLE_INDICES was set.
             */
            public Index[] concreteIndicesAsArray() {
                return this.concreteIndices().toArray(Index.EMPTY_ARRAY);
            }

            /**
             * Returns the concrete indices. This might throw an exception if there were issues during index resolution.
             * <p>
             * If you need access to index information while avoiding exceptions, you can use the following approaches:
             * <ul>
             *     <li>Use the names() method instead. This might also contain of non-existing indices or invalid names</li>
             *     <li>Use withoutResolutionErrors().concreteIndices(). This will only contain existing indices.</li>
             * </ul>
             *
             * @throws org.opensearch.index.IndexNotFoundException This exception is thrown for several conditions:
             * 1. If one of the index expression pointed to a missing index, alias or data stream and the IndicesOptions
             * used during resolution do not allow such a case. 2. If the set of resolved concrete indices is empty and
             * the IndicesOptions used during resolution do not allow such a case.
             * @throws IllegalArgumentException if one of the aliases resolved to multiple indices and
             * IndicesOptions.FORBID_ALIASES_TO_MULTIPLE_INDICES was set.
             */
            public Set<String> namesOfConcreteIndices() {
                return this.concreteIndices().stream().map(Index::getName).collect(Collectors.toSet());
            }

            /**
             * Returns the concrete indices. This might throw an exception if there were issues during index resolution.
             * <p>
             * If you need access to index information while avoiding exceptions, you can use the following approaches:
             * <ul>
             *     <li>Use the names() method instead. This might also contain of non-existing indices or invalid names</li>
             *     <li>Use withoutResolutionErrors().concreteIndices(). This will only contain existing indices.</li>
             * </ul>
             *
             * @throws org.opensearch.index.IndexNotFoundException This exception is thrown for several conditions:
             * 1. If one of the index expression pointed to a missing index, alias or data stream and the IndicesOptions
             * used during resolution do not allow such a case. 2. If the set of resolved concrete indices is empty and
             * the IndicesOptions used during resolution do not allow such a case.
             * @throws IllegalArgumentException if one of the aliases resolved to multiple indices and
             * IndicesOptions.FORBID_ALIASES_TO_MULTIPLE_INDICES was set.
             */
            public String[] namesOfConcreteIndicesAsArray() {
                return this.concreteIndices().stream().map(Index::getName).toArray(String[]::new);
            }

            @Override
            public ResolvedIndices.Local.Concrete withOriginalIndices(OriginalIndices originalIndices) {
                return new Concrete(this.concreteIndices, this.names, originalIndices, this.subActions, resolutionErrors);
            }

            @Override
            public ResolvedIndices.Local withSubActions(String actionType, ResolvedIndices.Local local) {
                Map<String, Local> subActions = new HashMap<>(this.subActions);
                subActions.put(actionType, local);
                return new Concrete(this.concreteIndices, this.names, this.originalIndices, subActions, resolutionErrors);
            }

            /**
             * Returns a new copy of this ResolvedIndices.Local.Concrete instances which has the given resolutionErrors
             * associated.
             */
            public ResolvedIndices.Local.Concrete withResolutionErrors(List<RuntimeException> resolutionErrors) {
                if (resolutionErrors.isEmpty()) {
                    return this;
                } else {
                    return new Concrete(
                        this.concreteIndices,
                        this.names(),
                        originalIndices,
                        this.subActions,
                        Stream.concat(this.resolutionErrors.stream(), resolutionErrors.stream()).toList()
                    );
                }
            }

            /**
             * Returns a new copy of this ResolvedIndices.Local.Concrete instances which has the given resolutionErrors
             * associated.
             */
            public ResolvedIndices.Local.Concrete withResolutionErrors(RuntimeException... resolutionErrors) {
                return withResolutionErrors(Arrays.asList(resolutionErrors));
            }

            /**
             * Returns a new copy of this ResolvedIndices.Local.Concrete instances which does not have any resolutionErrors
             * associated.
             */
            public ResolvedIndices.Local.Concrete withoutResolutionErrors() {
                return new Concrete(this.concreteIndices, this.names(), this.originalIndices, this.subActions, List.of());
            }

            @Override
            public boolean equals(Object other) {
                if (!super.equals(other)) {
                    return false;
                }

                if (!(other instanceof ResolvedIndices.Local.Concrete otherLocal)) {
                    return false;
                }

                return this.concreteIndices.equals(otherLocal.concreteIndices);
            }

            @Override
            public int hashCode() {
                return super.hashCode() * 31 + this.concreteIndices.hashCode();
            }

            List<RuntimeException> resolutionErrors() {
                return this.resolutionErrors;
            }

            private void checkResolutionErrors() {
                if (!this.resolutionErrors.isEmpty()) {
                    throw this.resolutionErrors.getFirst();
                }
            }

            @Override
            public String toString() {
                return "{" + "concreteIndices=" + concreteIndices + ", names=" + names() + ", resolutionErrors=" + resolutionErrors + '}';
            }
        }
    }

    /**
     * Represents the remote indices part of the respective request.
     */
    @ExperimentalApi
    public static class Remote {
        static final Remote EMPTY = new Remote(Collections.emptyMap(), Collections.emptyList());

        private final Map<String, OriginalIndices> clusterToOriginalIndicesMap;
        private List<String> rawExpressions;

        private Remote(Map<String, OriginalIndices> clusterToOriginalIndicesMap) {
            this.clusterToOriginalIndicesMap = clusterToOriginalIndicesMap;
        }

        private Remote(Map<String, OriginalIndices> clusterToOriginalIndicesMap, List<String> rawExpressions) {
            this.clusterToOriginalIndicesMap = clusterToOriginalIndicesMap;
            this.rawExpressions = rawExpressions;
        }

        /**
         * Returns a map with remote cluster names as keys and an OriginalIndices object specifying index expressions as values.
         */
        public Map<String, OriginalIndices> asClusterToOriginalIndicesMap() {
            return this.clusterToOriginalIndicesMap;
        }

        /**
         * Returns the remote clusters in the format ["remote_1:index_1", "remote_1:index_2", "remote_2:index_3"]
         */
        public List<String> asRawExpressions() {
            List<String> result = this.rawExpressions;

            if (result == null) {
                result = this.rawExpressions = buildRawExpressions();
            }

            return result;
        }

        /**
         * Returns the remote clusters in the format ["remote_1:index_1", "remote_1:index_2", "remote_2:index_3"]
         */
        public String[] asRawExpressionsArray() {
            return this.asRawExpressions().toArray(new String[0]);
        }

        /**
         * Returns true if no remote indices are present
         */
        public boolean isEmpty() {
            return this.clusterToOriginalIndicesMap.isEmpty();
        }

        @Override
        public String toString() {
            return this.asRawExpressions().toString();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResolvedIndices.Remote otherRemote)) {
                return false;
            }

            return this.asRawExpressions().equals(otherRemote.asRawExpressions());
        }

        @Override
        public int hashCode() {
            return this.asRawExpressions().hashCode();
        }

        private List<String> buildRawExpressions() {
            List<String> result = new ArrayList<>(this.clusterToOriginalIndicesMap.size());

            for (Map.Entry<String, OriginalIndices> entry : this.clusterToOriginalIndicesMap.entrySet()) {
                for (String remoteIndex : entry.getValue().indices()) {
                    result.add(RemoteClusterService.buildRemoteIndexName(entry.getKey(), remoteIndex));
                }
            }

            return Collections.unmodifiableList(result);
        }
    }
}
