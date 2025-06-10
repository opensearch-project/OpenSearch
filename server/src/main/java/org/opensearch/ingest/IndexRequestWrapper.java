/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;

import java.util.List;

/**
 * A wrapper for the index request to help execute the ingest pipelines.
 */
public class IndexRequestWrapper {
    /**
     * slot of the IndexRequestWrapper is the index of the request in the list of the requests.
     * It can be used to map the ingested result or exception to right index request.
     */
    private final int slot;

    /**
     * Child slot maps a index request to its potential parent action request.
     * For example, update requests with doc and upsert defined can have two child index requests
     * with the same slot but different results/failures, differentiated by their child slot
     */
    private final int childSlot;
    private final IndexRequest indexRequest;
    private final DocWriteRequest<?> actionRequest;
    private final List<IngestPipelineInfo> pipelineInfoList;

    public IndexRequestWrapper(
        int slot,
        int childSlot,
        IndexRequest indexRequest,
        DocWriteRequest<?> actionRequest,
        List<IngestPipelineInfo> pipelineInfoList
    ) {
        this.slot = slot;
        this.childSlot = childSlot;
        this.indexRequest = indexRequest;
        this.actionRequest = actionRequest;
        this.pipelineInfoList = pipelineInfoList;
    }

    public int getSlot() {
        return slot;
    }

    public int getChildSlot() {
        return childSlot;
    }

    public IndexRequest getIndexRequest() {
        return indexRequest;
    }

    public DocWriteRequest<?> getActionRequest() {
        return actionRequest;
    }

    public List<IngestPipelineInfo> getIngestPipelineInfoList() {
        return pipelineInfoList;
    }
}
