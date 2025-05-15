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
    private final IndexRequest indexRequest;
    private final DocWriteRequest<?> actionRequest;
    private final List<IngestPipelineInfo> pipelineInfoList;

    public IndexRequestWrapper(
        int slot,
        IndexRequest indexRequest,
        DocWriteRequest<?> actionRequest,
        List<IngestPipelineInfo> pipelineInfoList
    ) {
        this.slot = slot;
        this.indexRequest = indexRequest;
        this.actionRequest = actionRequest;
        this.pipelineInfoList = pipelineInfoList;
    }

    public int getSlot() {
        return slot;
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
