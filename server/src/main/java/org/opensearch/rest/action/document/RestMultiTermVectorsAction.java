/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.document;

import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to get a multi term vector
 *
 * @opensearch.api
 */
public class RestMultiTermVectorsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_mtermvectors"),
                new Route(POST, "/_mtermvectors"),
                new Route(GET, "/{index}/_mtermvectors"),
                new Route(POST, "/{index}/_mtermvectors")
            )
        );
    }

    @Override
    public String getName() {
        return "document_multi_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        TermVectorsRequest template = new TermVectorsRequest().index(request.param("index"));

        RestTermVectorsAction.readURIParameters(template, request);
        multiTermVectorsRequest.ids(Strings.commaDelimitedListToStringArray(request.param("ids")));
        request.withContentOrSourceParamParserOrNull(p -> multiTermVectorsRequest.add(template, p));

        return channel -> client.multiTermVectors(multiTermVectorsRequest, new RestToXContentListener<>(channel));
    }

}
