/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.pipeline.PipelinedRequest;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

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
public class QueryCoordinatorContext implements QueryRewriteContext {
    private final QueryRewriteContext rewriteContext;
    private final PipelinedRequest searchRequest;

    public QueryCoordinatorContext(QueryRewriteContext rewriteContext, PipelinedRequest searchRequest) {
        this.rewriteContext = rewriteContext;
        this.searchRequest = searchRequest;
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return rewriteContext.getXContentRegistry();
    }

    @Override
    public long nowInMillis() {
        return rewriteContext.nowInMillis();
    }

    @Override
    public NamedWriteableRegistry getWriteableRegistry() {
        return rewriteContext.getWriteableRegistry();
    }

    @Override
    public QueryShardContext convertToShardContext() {
        return rewriteContext.convertToShardContext();
    }

    @Override
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        rewriteContext.registerAsyncAction(asyncAction);
    }

    @Override
    public boolean hasAsyncActions() {
        return rewriteContext.hasAsyncActions();
    }

    @Override
    public void executeAsyncActions(ActionListener listener) {
        rewriteContext.executeAsyncActions(listener);
    }

    @Override
    public boolean validate() {
        return rewriteContext.validate();
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
