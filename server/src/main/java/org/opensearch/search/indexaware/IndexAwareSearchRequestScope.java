/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.tasks.Task;

import java.util.Collection;
import java.util.Map;

/**
 * Classifies the searches an {@code IndexAwareSearchRequestProcessor} does not act on by default.
 * <ul>
 *   <li>{@link #isInternalByDefault} identifies searches where changing the query is unsafe unless a
 *       processor explicitly opts in: the search behind a by-query or reindex job, and a search of a
 *       system index.</li>
 *   <li>{@link #hasRemoteIndices} identifies cross-cluster searches, which processors never run for.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class IndexAwareSearchRequestScope {

    private IndexAwareSearchRequestScope() {}

    /**
     * Whether a search with this parent task and these target indices is internal: one a processor only
     * runs for if it returns {@code true} from
     * {@code IndexAwareSearchRequestProcessor#appliesToInternalSearches()}.
     * <p>
     * A search is internal when either of the following holds:
     * <ul>
     *   <li>Its parent is a {@link BulkByScrollTask}, the task type shared by {@code _delete_by_query},
     *       {@code _update_by_query} and {@code _reindex}. The documents that search matches are the
     *       documents the job deletes, updates or copies, so a processor that changes what the query
     *       matches would change which documents the job touches.</li>
     *   <li>Any target index is a system index ({@link IndexMetadata#isSystem()}). Those searches are
     *       housekeeping reads by core or a plugin, not user queries.</li>
     * </ul>
     * The decision applies to the whole request, not to each index: a search over several indices, one of
     * which is a system index, is internal as a whole.
     * <p>
     * {@code _count} is not internal. It is an ordinary search with {@code size: 0} under the caller's own
     * task, so processors apply to it exactly as they do to the equivalent {@code _search}.
     * <p>
     * Only task types core knows about are recognized. A plugin whose own task type has the same concern
     * as {@link BulkByScrollTask} is not detected here.
     *
     * @param parentTask    the task that issued the search, or {@code null} if it has none or its parent
     *                      runs on another node
     * @param targetIndices the metadata of the local indices the search resolves to
     * @return {@code true} if processors must opt in to run for this search
     */
    public static boolean isInternalByDefault(@Nullable Task parentTask, Collection<IndexMetadata> targetIndices) {
        if (parentTask instanceof BulkByScrollTask) {
            return true;
        }
        for (IndexMetadata indexMetadata : targetIndices) {
            if (indexMetadata.isSystem()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a search targets any index on a remote cluster. Processors do not run for such a search,
     * whatever its {@code ccs_minimize_roundtrips} setting, because the coordinating node sends the source
     * it holds to the remote side in both modes:
     * <ul>
     *   <li>without round-trip minimization, directly to the remote shards, whose mappings the local
     *       {@code IndexAwareContext} knows nothing about;</li>
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
