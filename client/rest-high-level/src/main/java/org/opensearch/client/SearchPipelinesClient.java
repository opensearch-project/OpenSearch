/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineResponse;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptySet;

public final class SearchPipelinesClient {
    private final RestHighLevelClient restHighLevelClient;

    SearchPipelinesClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Add a pipeline or update an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putPipeline(PutSearchPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            SearchPipelinesRequestConverters::putPipeline,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously add a pipeline or update an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putPipelineAsync(
        PutSearchPipelineRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            SearchPipelinesRequestConverters::putPipeline,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Get an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetSearchPipelineResponse getPipeline(GetSearchPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            SearchPipelinesRequestConverters::getPipeline,
            options,
            GetSearchPipelineResponse::fromXContent,
            Collections.singleton(404)
        );
    }

    /**
     * Asynchronously get an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getPipelineAsync(
        GetSearchPipelineRequest request,
        RequestOptions options,
        ActionListener<GetSearchPipelineResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            SearchPipelinesRequestConverters::getPipeline,
            options,
            GetSearchPipelineResponse::fromXContent,
            listener,
            Collections.singleton(404)
        );
    }

    /**
     * Delete an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deletePipeline(DeleteSearchPipelineRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            SearchPipelinesRequestConverters::deletePipeline,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously delete an existing pipeline.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deletePipelineAsync(
        DeleteSearchPipelineRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            SearchPipelinesRequestConverters::deletePipeline,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

}
