/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context class to hold indices to be tiered per request. It also holds
 * the listener per request to mark the request as complete once all
 * tiering operations are completed.
 *
 * @opensearch.experimental
 */

@ExperimentalApi
public class TieringRequestContext {
    private final ActionListener<HotToWarmTieringResponse> actionListener;
    private final Set<Index> inProgressIndices;
    private final Set<Index> tieredIndices;
    private final Set<Index> completedIndices;
    private final Map<Index, String> failedIndices;

    public TieringRequestContext(
        ActionListener<HotToWarmTieringResponse> actionListener,
        Set<Index> acceptedIndices,
        Map<Index, String> failedIndices
    ) {
        this.actionListener = actionListener;
        // by default all the accepted indices are added to the in-progress set
        this.inProgressIndices = ConcurrentHashMap.newKeySet();
        inProgressIndices.addAll(acceptedIndices);
        this.failedIndices = failedIndices;
        this.tieredIndices = new HashSet<>();
        this.completedIndices = new HashSet<>();
    }

    public ActionListener<HotToWarmTieringResponse> getListener() {
        return actionListener;
    }

    public Map<Index, String> getFailedIndices() {
        return failedIndices;
    }

    public Set<Index> getInProgressIndices() {
        return inProgressIndices;
    }

    public Set<Index> getCompletedIndices() {
        return completedIndices;
    }

    public Set<Index> getTieredIndices() {
        return tieredIndices;
    }

    public boolean isRequestProcessingComplete() {
        return inProgressIndices.isEmpty() && tieredIndices.isEmpty();
    }

    public void addToFailed(Index index, String reason) {
        inProgressIndices.remove(index);
        failedIndices.put(index, reason);
    }

    public void addToTiered(Index index) {
        inProgressIndices.remove(index);
        tieredIndices.add(index);
    }

    public void addToCompleted(Index index) {
        tieredIndices.remove(index);
        completedIndices.add(index);
    }

    @Override
    public String toString() {
        return "TieringRequestContext{"
            + "actionListener="
            + actionListener
            + ", inProgressIndices="
            + inProgressIndices
            + ", tieredIndices="
            + tieredIndices
            + ", completedIndices="
            + completedIndices
            + ", failedIndices="
            + failedIndices
            + '}';
    }
}
