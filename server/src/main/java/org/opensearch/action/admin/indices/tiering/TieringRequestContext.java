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
import org.opensearch.indices.tiering.IndexTieringState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private final Map<Index, IndexTieringInfo> indexTieringStatusMap;

    public TieringRequestContext(
        ActionListener<HotToWarmTieringResponse> actionListener,
        Set<Index> acceptedIndices,
        Map<Index, String> failedIndices
    ) {
        this.actionListener = actionListener;
        indexTieringStatusMap = new ConcurrentHashMap<>();
        for (Index index : acceptedIndices) {
            indexTieringStatusMap.put(index, new IndexTieringInfo());
        }
        for (Map.Entry<Index, String> entry : failedIndices.entrySet()) {
            indexTieringStatusMap.put(entry.getKey(), new IndexTieringInfo(IndexTieringState.FAILED, entry.getValue()));
        }
    }

    public ActionListener<HotToWarmTieringResponse> getListener() {
        return actionListener;
    }

    public boolean hasIndex(Index index) {
        return indexTieringStatusMap.containsKey(index);
    }

    public Map<Index, String> getFailedIndices() {
        Map<Index, String> failedIndicesMap = new HashMap<>();
        for (Index index : filterIndicesByState(IndexTieringState.FAILED)) {
            failedIndicesMap.put(index, indexTieringStatusMap.get(index).getReason());
        }
        return failedIndicesMap;
    }

    public boolean isRequestProcessingComplete() {
        return filterIndicesByState(IndexTieringState.COMPLETED).size() + filterIndicesByState(IndexTieringState.FAILED)
            .size() == indexTieringStatusMap.size();
    }

    public List<Index> filterIndicesByState(IndexTieringState state) {
        return indexTieringStatusMap.keySet()
            .stream()
            .filter(indexTieringInfo -> indexTieringStatusMap.get(indexTieringInfo).getState() == state)
            .collect(Collectors.toList());
    }

    public List<Index> getIndicesPendingTiering() {
        return indexTieringStatusMap.keySet()
            .stream()
            .filter(indexTieringInfo -> indexTieringStatusMap.get(indexTieringInfo).getState() == IndexTieringState.PENDING_START)
            .collect(Collectors.toList());
    }

    public void markIndexFailed(Index index, String reason) {
        indexTieringStatusMap.get(index).markFailed(reason);
    }

    public void markIndexInProgress(Index index) {
        indexTieringStatusMap.get(index).markInProgress();
    }

    public void markIndexAsPendingComplete(Index index) {
        indexTieringStatusMap.get(index).markAsPendingComplete();
    }

    public void markIndexAsCompleted(Index index) {
        indexTieringStatusMap.get(index).markCompleted();
    }

    public boolean hasIndexFailed(Index index) {
        return indexTieringStatusMap.get(index).hasFailed();
    }

    @Override
    public String toString() {
        return "TieringRequestContext{" + "actionListener=" + actionListener + ", indexTieringStatusMap=" + indexTieringStatusMap + '}';
    }

    /**
     * Represents info of a tiering index
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class IndexTieringInfo {
        private IndexTieringState state;
        private String reason;

        public IndexTieringInfo() {
            this.state = IndexTieringState.PENDING_START;
            this.reason = null;
        }

        public IndexTieringInfo(IndexTieringState state, String reason) {
            this.state = state;
            this.reason = reason;
        }

        public IndexTieringState getState() {
            return state;
        }
        public void markInProgress() {
            this.state = IndexTieringState.IN_PROGRESS;
        }

        public void markAsPendingComplete() {
            this.state = IndexTieringState.PENDING_COMPLETION;
        }

        public void markCompleted() {
            this.state = IndexTieringState.COMPLETED;
        }

        public void markFailed(String reason) {
            this.state = IndexTieringState.FAILED;
            this.reason = reason;
        }

        public String getReason() {
            return reason;
        }

        public boolean hasFailed() {
            return state.failed();
        }

        @Override
        public String toString() {
            return "IndexTieringInfo{" +
                "state=" + state +
                ", reason='" + reason + '\'' +
                '}';
        }
    }
}
