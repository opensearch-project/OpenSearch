/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.OriginalIndices;

import java.util.Map;

/**
 * Classifies the searches an {@link IndexAwareSearchRequestProcessor} never runs for: cross-cluster
 * searches ({@link #hasRemoteIndices}).
 *
 * @opensearch.internal
 */
public final class IndexAwareSearchRequestScope {

    private IndexAwareSearchRequestScope() {}

    /**
     * Whether a search targets any index on a remote cluster. Processors do not run for such a search,
     * whatever its {@code ccs_minimize_roundtrips} setting, because the coordinating node sends the source
     * it holds to the remote side in both modes:
     * <ul>
     *   <li>without round-trip minimization, directly to the remote shards, whose mappings the local
     *       {@link IndexAwareContext} knows nothing about;</li>
     *   <li>with it, to a remote coordinating node, which would run its own processors on a source the
     *       local processors had already changed.</li>
     * </ul>
     * The local indices of a search that mixes local and remote indices are therefore not processed
     * either.
     *
     * @param remoteIndices the search's remote indices, keyed by cluster alias
     * @return {@code true} if the search targets at least one remote index
     */
    public static boolean hasRemoteIndices(Map<String, OriginalIndices> remoteIndices) {
        return remoteIndices.isEmpty() == false;
    }
}
