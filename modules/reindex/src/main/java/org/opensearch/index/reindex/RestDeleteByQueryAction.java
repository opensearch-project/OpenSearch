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

package org.opensearch.index.reindex;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestDeleteByQueryAction extends AbstractBulkByQueryRestHandler<DeleteByQueryRequest, DeleteByQueryAction> {

    public RestDeleteByQueryAction() {
        super(DeleteByQueryAction.INSTANCE);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_delete_by_query")));
    }

    @Override
    public String getName() {
        return "delete_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, false);
    }

    @Override
    protected DeleteByQueryRequest buildRequest(RestRequest request, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        /*
         * Passing the search request through DeleteByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parseInternalRequest can override them.
         */
        DeleteByQueryRequest internal = new DeleteByQueryRequest();

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("max_docs", s -> setMaxDocsValidateIdentical(internal, ((Number) s).intValue()));

        parseInternalRequest(internal, request, namedWriteableRegistry, consumers);

        return internal;
    }
}
