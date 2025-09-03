/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A class that possibly encapsulates resolved indices. See the documentation of {@link ResolvedIndices} for a full
 * description.
 * <p>
 * In contrast to ResolvedIndices, objects of this class may convey the message "the resolved indices are unknown".
 * This may be for several reasons:
 * <ul>
 *     <li>The information can be only obtained on a master node</li>
 *     <li>The particular action does not implement the TransportIndicesResolvingAction interface</li>
 *     <li>It is infeasible to collect the information</li>
 * </ul>
 * For authorization purposes, the case of unknown resolved indices should be usually treated as a "requires
 * privileges for all indices" case.
 * <p>
 * The class {@link ResolvedIndices} extends OptionallyResolvedIndices. A safe usage pattern would be thus:
 * <pre>
 *     if (optionallyResolvedIndices instanceof ResolvedIndices resolvedIndices) {
 *         Set&lt;String;gt names = resolvedIndices.local().names();
 *     }
 * </pre>
 */
@ExperimentalApi
public class OptionallyResolvedIndices {
    private static final OptionallyResolvedIndices NOT_PRESENT = new OptionallyResolvedIndices();

    public static OptionallyResolvedIndices unknown() {
        return NOT_PRESENT;
    }

    public OptionallyResolvedIndices.Local local() {
        return Local.NOT_PRESENT;
    }

    @Override
    public String toString() {
        return "ResolvedIndices{unknown=true}";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OptionallyResolvedIndices otherResolvedIndices)) {
            return false;
        }

        return this.local() == otherResolvedIndices.local();
    }

    @Override
    public int hashCode() {
        return 92;
    }

    /**
     * Represents the local (i.e., non-remote) indices referenced by the respective request.
     */
    @ExperimentalApi
    public static class Local {
        private static final OptionallyResolvedIndices.Local NOT_PRESENT = new OptionallyResolvedIndices.Local();

        /**
         * Returns all the local names. These names might be indices, aliases or data streams, depending on the usage.
         * <p>
         * In case this is an isUnknown() object, this will return a set of all concrete indices on the cluster (incl.
         * hidden and closed indices). This might be a large object. Be prepared to handle such a large object.
         * <p>
         * <strong>This method will be only rarely needed. In most cases <code>ResolvedIndices.names()</code> will be sufficient.</strong>
         */
        public Set<String> names(ClusterState clusterState) {
            return clusterState.metadata().getIndicesLookup().keySet();
        }

        /**
         * Returns always true. For the unknown resolved indices, we always assume that these are not empty.
         */
        public boolean isEmpty() {
            return false;
        }

        /**
         * Returns always true, as we need to assume that an index is potentially contained if the set of indices is unknown.
         */
        public boolean contains(String name) {
            return true;
        }

        /**
         * Returns always true, as we need to assume that an index is potentially contained if the set of indices is unknown.
         */
        public boolean containsAny(Collection<String> names) {
            return true;
        }

        /**
         * Returns always true, as we need to assume that an index is potentially contained if the set of indices is unknown.
         */
        public boolean containsAny(Predicate<String> namePredicate) {
            return true;
        }
    }
}
