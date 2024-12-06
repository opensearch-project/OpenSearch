/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.client.Client;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.pipeline.PipelinedRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * QueryCoordinatorContext
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

        Map<String, Object> contextVariables = new HashMap<>();

        // Read from pipeline context
        contextVariables.putAll(searchRequest.getPipelineProcessingContext().getAttributes());

        return contextVariables;
    }
}
