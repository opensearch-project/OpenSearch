/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.query.QueryBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Effective document-level read restrictions grouped by concrete indices that share the same restrictions.
 * A policy returned by {@link ReadAccessPolicyProvider#getDocumentLevelReadAccessPolicy(ReadAccessContext)} must
 * retain any request-specific authorization information it needs. Its methods must not depend on the calling thread's
 * context, so the policy can be used later on another thread during the same request. Restrictions may be computed
 * when the policy is created or when its methods are called.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentLevelReadAccessPolicy {

    /** Returns an unrestricted read-access policy. */
    static DocumentLevelReadAccessPolicy unrestricted() {
        return UnrestrictedDocumentLevelReadAccessPolicy.INSTANCE;
    }

    /** Returns whether at least one concrete index is protected. */
    boolean hasRestrictions();

    /**
     * Returns the names of the concrete indices that are covered by this instance.
     */
    Set<String> coveredConcreteIndices();

    /**
     * Restrictions that are shared by a set of concrete indices. This is useful for multi-index reads and avoids splitting up queries into huge unions.
     */
    @ExperimentalApi
    interface IndexGroup {
        Set<String> concreteIndices();

        Optional<QueryBuilder> restrictions();
    }

    /**
     * Returns just the restrictions for one concrete index.
     */
    Optional<QueryBuilder> restrictionsForIndex(String concreteIndex);

    /**
     * Returns all index groups in this policy. The union of groups is guaranteed to cover all indices listed in
     * coveredConcreteIndices(), no matter whether there are restrictions or not.
     */
    Collection<IndexGroup> indexGroups();
}

final class UnrestrictedDocumentLevelReadAccessPolicy implements DocumentLevelReadAccessPolicy {
    static final DocumentLevelReadAccessPolicy INSTANCE = new UnrestrictedDocumentLevelReadAccessPolicy();

    private UnrestrictedDocumentLevelReadAccessPolicy() {}

    @Override
    public boolean hasRestrictions() {
        return false;
    }

    @Override
    public Set<String> coveredConcreteIndices() {
        return Set.of();
    }

    @Override
    public Optional<QueryBuilder> restrictionsForIndex(String concreteIndex) {
        return Optional.empty();
    }

    @Override
    public Collection<IndexGroup> indexGroups() {
        return List.of();
    }
}
