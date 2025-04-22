/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.pipeline.PipelinedRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * The QueryCoordinatorContext class implements the QueryRewriteContext interface and provides
 * additional functionality for coordinating query rewriting in OpenSearch.
 *
 * This class acts as a wrapper around a QueryRewriteContext instance and a PipelinedRequest,
 * allowing access to both rewrite context methods and pass over search request information.
 *
 * @since 2.19.0
 */
@PublicApi(since = "2.19.0")
public class QueryCoordinatorContext extends QueryRewriteContext {
    private final PipelinedRequest searchRequest;

    public QueryCoordinatorContext(QueryRewriteContext parent, PipelinedRequest pipelinedRequest) {
        super(parent.getXContentRegistry(), parent.getWriteableRegistry(), parent.client, parent.nowInMillis, parent.validate());
        this.searchRequest = pipelinedRequest;
    }

    @Override
    public QueryCoordinatorContext convertToCoordinatorContext() {
        return this;
    }

    public Map<String, Object> getContextVariables() {

        // Read from pipeline context
        Map<String, Object> contextVariables = new HashMap<>(searchRequest.getPipelineProcessingContext().getAttributes());

        return contextVariables;
    }
}
