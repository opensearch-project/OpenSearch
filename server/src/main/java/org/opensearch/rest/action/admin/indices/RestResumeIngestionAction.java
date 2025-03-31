/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to resume pull-based ingestion.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestResumeIngestionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/ingestion/_resume")));
    }

    @Override
    public String getName() {
        return "resume_ingestion_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ResumeIngestionRequest resumeIngestionRequest = new ResumeIngestionRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        resumeIngestionRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", resumeIngestionRequest.clusterManagerNodeTimeout())
        );
        resumeIngestionRequest.timeout(request.paramAsTime("timeout", resumeIngestionRequest.timeout()));
        resumeIngestionRequest.indicesOptions(IndicesOptions.fromRequest(request, resumeIngestionRequest.indicesOptions()));

        return channel -> client.admin().indices().resumeIngestion(resumeIngestionRequest, new RestToXContentListener<>(channel));
    }

}
