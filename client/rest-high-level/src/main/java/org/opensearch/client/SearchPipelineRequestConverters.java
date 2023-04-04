/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineRequest;
import org.opensearch.action.search.PutSearchPipelineRequest;

import java.io.IOException;

final class SearchPipelineRequestConverters {
    private SearchPipelineRequestConverters() {}

    static Request putPipeline(PutSearchPipelineRequest putPipelineRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_search/pipeline")
            .addPathPart(putPipelineRequest.getId())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(putPipelineRequest.timeout());
        params.withClusterManagerTimeout(putPipelineRequest.clusterManagerNodeTimeout());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putPipelineRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deletePipeline(DeleteSearchPipelineRequest deletePipelineRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_search/pipeline")
            .addPathPart(deletePipelineRequest.getId())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(deletePipelineRequest.timeout());
        parameters.withClusterManagerTimeout(deletePipelineRequest.clusterManagerNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request getPipeline(GetSearchPipelineRequest getPipelineRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_search/pipeline")
            .addCommaSeparatedPathParts(getPipelineRequest.getIds())
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withClusterManagerTimeout(getPipelineRequest.clusterManagerNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }
}
