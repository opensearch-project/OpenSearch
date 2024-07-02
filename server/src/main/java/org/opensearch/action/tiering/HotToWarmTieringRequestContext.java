/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.tiering;

import org.opensearch.common.UUIDs;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context class to hold indices to be tiered per request. It also holds
 * the listener per request to mark the request as complete once all
 * tiering operations are completed.
 *
 * @opensearch.experimental
 */

public class HotToWarmTieringRequestContext {
    private Set<Index> acceptedIndices;
    private Map<String, String> rejectedIndices;
    private Set<String> notFoundIndices;
    private Set<Index> inProgressIndices;
    private Set<String> successfulIndices;
    private Set<String> failedIndices;
    private ActionListener<HotToWarmTieringResponse> actionListener;
    private TieringIndexRequest request;
    private String requestUuid;

    public HotToWarmTieringRequestContext() {
        this.acceptedIndices = new HashSet<>();
        this.rejectedIndices = new HashMap<>();
        this.notFoundIndices = new HashSet<>();
        this.successfulIndices = new HashSet<>();
        this.failedIndices = new HashSet<>();
        this.inProgressIndices = new HashSet<>();
        this.requestUuid = UUIDs.randomBase64UUID();
    }

    public HotToWarmTieringRequestContext(ActionListener<HotToWarmTieringResponse> actionListener, TieringIndexRequest request) {
        this();
        this.actionListener = actionListener;
        this.request = request;
    }

    public HotToWarmTieringRequestContext(Set<Index> acceptedIndices, Map<String, String> rejectedIndices, Set<String> notFoundIndices) {
        this.acceptedIndices = acceptedIndices;
        this.rejectedIndices = rejectedIndices;
        this.notFoundIndices = notFoundIndices;
    }

    public Set<Index> getAcceptedIndices() {
        return acceptedIndices;
    }

    public void setAcceptedIndices(Set<Index> acceptedIndices) {
        this.acceptedIndices = acceptedIndices;
    }

    public Map<String, String> getRejectedIndices() {
        return rejectedIndices;
    }

    public Set<String> getNotFoundIndices() {
        return notFoundIndices;
    }

    public void setNotFoundIndices(Set<String> notFoundIndices) {
        this.notFoundIndices = notFoundIndices;
    }

    public void setRejectedIndices(Map<String, String> rejectedIndices) {
        this.rejectedIndices = rejectedIndices;
    }

    public void addToNotFound(String indexName) {
        notFoundIndices.add(indexName);
    }

    /**
     * Method to move indices from success list to failed list with corresponding reasons to be sent in response
     * @param indicesToFail - sets of indices that failed validation with same reason
     * @param failureReason - reason for not accepting migration for this index
     */
    public void addToRejected(Set<String> indicesToFail, String failureReason) {
        for (String index : indicesToFail) {
            addToRejected(index, failureReason);
        }
    }

    /**
     * Method to move index from success list to failed list with corresponding reasons to be sent in response
     * @param indexName - indexName that failed validation
     * @param failureReason - reason for not accepting migration for this index
     */
    public void addToRejected(String indexName, String failureReason) {
        rejectedIndices.put(indexName, failureReason);
    }

    public void addToAccepted(Index index) {
        acceptedIndices.add(index);
    }

    public void addToAccepted(Set<Index> indices) {
        acceptedIndices.addAll(indices);
    }

    public Set<String> getSuccessfulIndices() {
        return successfulIndices;
    }

    public void setSuccessfulIndices(Set<String> successfulIndices) {
        this.successfulIndices = successfulIndices;
    }

    public void addToSuccessful(String index) {
        successfulIndices.add(index);
    }

    public Set<String> getFailedIndices() {
        return failedIndices;
    }

    public void setFailedIndices(Set<String> failedIndices) {
        this.failedIndices = failedIndices;
    }

    public void addToFailed(String index) {
        failedIndices.add(index);
    }

    public ActionListener<HotToWarmTieringResponse> getListener() {
        return actionListener;
    }

    public void setActionListener(ActionListener<HotToWarmTieringResponse> actionListener) {
        this.actionListener = actionListener;
    }

    public TieringIndexRequest getRequest() {
        return request;
    }

    public void setRequest(TieringIndexRequest request) {
        this.request = request;
    }

    public String getRequestUuid() {
        return requestUuid;
    }

    public void setRequestUuid(String requestUuid) {
        this.requestUuid = requestUuid;
    }

    public Set<Index> getInProgressIndices() {
        return inProgressIndices;
    }

    public void setInProgressIndices(Set<Index> inProgressIndices) {
        this.inProgressIndices = inProgressIndices;
    }

    public void addToInProgress(Index index) {
        inProgressIndices.add(index);
    }

    public void removeFromInProgress(Index index) {
        inProgressIndices.remove(index);
    }

    public boolean isRequestProcessingComplete() {
        return inProgressIndices.isEmpty();
    }

    public HotToWarmTieringResponse constructResponse() {
        final List<HotToWarmTieringResponse.IndexResult> indicesResult = new LinkedList<>();
        for (Map.Entry<String, String> rejectedIndex : rejectedIndices.entrySet()) {
            indicesResult.add(new HotToWarmTieringResponse.IndexResult(rejectedIndex.getKey(), rejectedIndex.getValue()));
        }
        for (String index : notFoundIndices) {
            indicesResult.add(new HotToWarmTieringResponse.IndexResult(index, "Index not found"));
        }
        for (String index : failedIndices) {
            indicesResult.add(new HotToWarmTieringResponse.IndexResult(index, "Index failed"));
        }
        return new HotToWarmTieringResponse(acceptedIndices.size() > 0, indicesResult);
    }

    @Override
    public String toString() {
        return "HotToWarmTieringRequestContext{"
            + "acceptedIndices="
            + acceptedIndices
            + ", rejectedIndices="
            + rejectedIndices
            + ", notFoundIndices="
            + notFoundIndices
            + ", inProgressIndices="
            + inProgressIndices
            + ", successfulIndices="
            + successfulIndices
            + ", failedIndices="
            + failedIndices
            + ", actionListener="
            + actionListener
            + ", request="
            + request
            + ", requestUuid='"
            + requestUuid
            + '\''
            + '}';
    }
}
