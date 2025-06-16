/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.ClusterState;
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
public class ResolvedIndices {
    public static ResolvedIndices of(String... indices) {
        return new ResolvedIndices(
            new Local(Collections.unmodifiableSet(new HashSet<>(Arrays.asList(indices))), null, false),
            Remote.EMPTY
        );
    }

    public static ResolvedIndices of(Index... indices) {
        return new ResolvedIndices(
            new Local(Stream.of(indices).map(Index::getName).collect(Collectors.toUnmodifiableSet()), null, false),
            Remote.EMPTY
        );
    }

    public static ResolvedIndices of(Collection<String> indices) {
        return new ResolvedIndices(new Local(Collections.unmodifiableSet(new HashSet<>(indices)), null, false), Remote.EMPTY);
    }

    public static ResolvedIndices all() {
        return ALL;
    }

    public static ResolvedIndices ofNonNull(String... indices) {
        Set<String> indexSet = new HashSet<>(indices.length);

        for (String index : indices) {
            if (index != null) {
                indexSet.add(index);
            }
        }

        return new ResolvedIndices(new Local(Collections.unmodifiableSet(indexSet), null, false), Remote.EMPTY);
    }

    private static final ResolvedIndices ALL = new ResolvedIndices(new Local(Set.of(Metadata.ALL), null, true), Remote.EMPTY);

    private final Local local;
    private final Remote remote;

    private ResolvedIndices(Local local, Remote remote) {
        this.local = local;
        this.remote = remote;
    }

    public Local local() {
        return this.local;
    }

    public Remote remote() {
        return this.remote;
    }

    public ResolvedIndices withRemoteIndices(Map<String, OriginalIndices> remoteIndices) {
        if (remoteIndices.isEmpty()) {
            return this;
        }

        Map<String, OriginalIndices> newRemoteIndices = new HashMap<>(remoteIndices);
        newRemoteIndices.putAll(this.remote.clusterToOriginalIndicesMap);

        return new ResolvedIndices(this.local, new Remote(Collections.unmodifiableMap(newRemoteIndices)));
    }

    public ResolvedIndices withLocalOriginalIndices(OriginalIndices originalIndices) {
        return new ResolvedIndices(new Local(this.local.names, originalIndices, this.local.isAll), this.remote);
    }

    public boolean isEmpty() {
        return this.local.isEmpty() && this.remote.isEmpty();
    }

    /**
     * Represents the local (i.e., non-remote) indices referenced by the respective request.
     */
    public static class Local {
        private final Set<String> names;
        private final OriginalIndices originalIndices;
        private final boolean isAll;

        private Local(Set<String> names, OriginalIndices originalIndices, boolean isAll) {
            this.names = names;
            this.originalIndices = originalIndices;
            this.isAll = isAll;
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         * <p>
         * <strong>Note: You must gate this call by <code>if (!isAll())</code>.</strong>
         * If isAll() is true, this method will throw an IllegalStateException. If you are sure that you really need all index names, please use the method
         * <code>names(ClusterState)</code> instead.
         *
         * @return an unmodifiable set of names of indices, aliases and/or data streams.
         * @throws IllegalStateException if isAll() is true
         */
        public Set<String> names() {
            if (this.isAll) {
                throw new IllegalStateException("ResolvedIndices.Local.names() cannot be called for isAll cases");
            }

            return this.names;
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         * <p>
         * <strong>Note: You must gate this call by <code>if (!isAll())</code>.</strong>
         * If isAll() is true, this method will throw an IllegalStateException. If you are sure that you really need all index names, please use the method
         * <code>names(ClusterState)</code> instead.
         *
         * @return an array of names of indices, aliases and/or data streams.
         * @throws IllegalStateException if isAll() is true
         */
        public String[] namesAsArray() {
            return this.names().toArray(new String[0]);
        }

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         * <p>
         * In case this is an isAll() object, this will return a set of all concrete indices on the cluster (incl.
         * hidden and closed indices). This might be a large object. Be prepared to handle such a large object.
         * <p>
         * <strong>This method will be only rarely needed. In most cases <code>names()</code> will be sufficient.</strong>
         */
        public Set<String> names(ClusterState clusterState) {
            if (this.isAll) {
                return clusterState.metadata().getIndicesLookup().keySet();
            } else {
                return this.names;
            }
        }

        public OriginalIndices originalIndices() {
            return this.originalIndices;
        }

        public boolean isEmpty() {
            if (this.isAll) {
                return false;
            } else {
                return this.names.isEmpty();
            }
        }

        public boolean isAll() {
            return this.isAll;
        }

        public boolean contains(String index) {
            if (this.isAll) {
                return true;
            } else {
                return this.names.contains(index);
            }
        }

        public boolean containsAny(Collection<String> indices) {
            if (this.isAll) {
                return true;
            } else {
                return indices.stream().anyMatch(this.names::contains);
            }
        }

        public boolean containsAny(Predicate<String> indexNamePredicate) {
            return this.names.stream().anyMatch(indexNamePredicate);
        }
    }

    /**
     * Represents the remote indices part of the respective request.
     */
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

        public Map<String, OriginalIndices> asClusterToOriginalIndicesMap() {
            return this.clusterToOriginalIndicesMap;
        }

        public List<String> asRawExpressions() {
            List<String> result = this.rawExpressions;

            if (result == null) {
                result = this.rawExpressions = buildRawExpressions();
            }

            return result;
        }

        public String[] asRawExpressionsArray() {
            return this.asRawExpressions().toArray(new String[0]);
        }

        public boolean isEmpty() {
            return this.clusterToOriginalIndicesMap.isEmpty();
        }

        private List<String> buildRawExpressions() {
            if (this.clusterToOriginalIndicesMap.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> result = new ArrayList<>();

            for (Map.Entry<String, OriginalIndices> entry : this.clusterToOriginalIndicesMap.entrySet()) {
                for (String remoteIndex : entry.getValue().indices()) {
                    result.add(RemoteClusterService.buildRemoteIndexName(entry.getKey(), remoteIndex));
                }
            }

            return Collections.unmodifiableList(result);
        }
    }

}
