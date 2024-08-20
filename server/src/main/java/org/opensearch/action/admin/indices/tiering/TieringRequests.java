/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;
import org.opensearch.indices.tiering.IndexTieringState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Tiering requests
 */
@ExperimentalApi
public class TieringRequests {
    private final Set<TieringRequestContext> acceptedTieringRequestContexts;
    private final Queue<TieringRequestContext> queuedTieringContexts;
    private final int queueSize;

    public TieringRequests(int queueSize) {
        this.queueSize = queueSize;
        acceptedTieringRequestContexts = new HashSet<>();
        queuedTieringContexts = new LinkedList<>();
    }

    public Set<TieringRequestContext> getAcceptedTieringRequestContexts() {
        return acceptedTieringRequestContexts;
    }


    public Queue<TieringRequestContext> getQueuedTieringContexts() {
        return queuedTieringContexts;
    }

    public void addToPending(TieringRequestContext tieringRequestContext) {
        queuedTieringContexts.add(tieringRequestContext);
    }

    public void addToPending(Set<TieringRequestContext> tieringRequestContexts) {
        for (TieringRequestContext tieringRequestContext : tieringRequestContexts) {
            addToPending(tieringRequestContext);
        }
    }

    public void addToAccepted(TieringRequestContext tieringRequestContext) {
        acceptedTieringRequestContexts.add(tieringRequestContext);
    }

    public void addToAccepted(Set<TieringRequestContext> tieringRequestContexts) {
        for (TieringRequestContext tieringRequestContext : tieringRequestContexts) {
            addToAccepted(tieringRequestContext);
        }
    }

    public Set<TieringRequestContext> dequePendingTieringContexts() {
        final Set<TieringRequestContext> tieringRequestContexts = new HashSet<>();
        int i = queueSize - acceptedTieringRequestContexts.size();
        while (!queuedTieringContexts.isEmpty() && i-- > 0) {
            tieringRequestContexts.add(queuedTieringContexts.poll());
        }
        return tieringRequestContexts;
    }

    public Map<Index, TieringRequestContext> getIndices(Set<TieringRequestContext> tieringRequestContexts, IndexTieringState state) {
        final Map<Index, TieringRequestContext> indexTieringRequestContextMap = new HashMap<>();
        for (TieringRequestContext tieringRequestContext : tieringRequestContexts) {
            for (Index index : tieringRequestContext.filterIndicesByState(state)) {
                indexTieringRequestContextMap.put(index, tieringRequestContext);
            }
        }
        return indexTieringRequestContextMap;
    }

    public boolean isEmpty() {
        return queuedTieringContexts.isEmpty() && acceptedTieringRequestContexts.isEmpty();
    }
}
