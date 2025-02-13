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

package org.opensearch.index.rankeval;

import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
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
 *  {
 *   "requests": [{
 *           "id": "amsterdam_query",
 *           "request": {
 *               "query": {
 *                   "match": {
 *                       "text": "amsterdam"
 *                   }
 *               }
 *          },
 *          "ratings": [{
 *                   "_index": "foo",
 *                   "_id": "doc1",
 *                   "rating": 0
 *               },
 *               {
 *                   "_index": "foo",
 *                   "_id": "doc2",
 *                   "rating": 1
 *               },
 *               {
 *                   "_index": "foo",
 *                   "_id": "doc3",
 *                   "rating": 1
 *               }
 *           ]
 *       },
 *       {
 *           "id": "berlin_query",
 *           "request": {
 *               "query": {
 *                   "match": {
 *                       "text": "berlin"
 *                   }
 *               },
 *               "size": 10
 *           },
 *           "ratings": [{
 *               "_index": "foo",
 *               "_id": "doc1",
 *               "rating": 1
 *           }]
 *       }
 *   ],
 *   "metric": {
 *       "precision": {
 *           "ignore_unlabeled": true
 *       }
 *   }
 * }
 */
public class RestRankEvalAction extends BaseRestHandler {

    public static String ENDPOINT = "_rank_eval";

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/" + ENDPOINT),
                new Route(POST, "/" + ENDPOINT),
                new Route(GET, "/{index}/" + ENDPOINT),
                new Route(POST, "/{index}/" + ENDPOINT)
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RankEvalRequest rankEvalRequest = new RankEvalRequest();
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parseRankEvalRequest(rankEvalRequest, request, parser);
        }
        return channel -> client.executeLocally(
            RankEvalAction.INSTANCE,
            rankEvalRequest,
            new RestToXContentListener<RankEvalResponse>(channel)
        );
    }

    private static void parseRankEvalRequest(RankEvalRequest rankEvalRequest, RestRequest request, XContentParser parser) {
        rankEvalRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        rankEvalRequest.indicesOptions(IndicesOptions.fromRequest(request, rankEvalRequest.indicesOptions()));
        if (request.hasParam("search_type")) {
            rankEvalRequest.searchType(SearchType.fromString(request.param("search_type")));
        }
        RankEvalSpec spec = RankEvalSpec.parse(parser);
        rankEvalRequest.setRankEvalSpec(spec);
    }

    @Override
    public String getName() {
        return "rank_eval_action";
    }
}
