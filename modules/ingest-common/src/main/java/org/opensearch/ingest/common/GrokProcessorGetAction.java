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

package org.opensearch.ingest.common;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.LocalOnlyActionRequest;
import org.opensearch.action.LocalOnlyActionResponse;
import org.opensearch.action.LocalOnlyActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.grok.Grok;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class GrokProcessorGetAction extends LocalOnlyActionType<GrokProcessorGetAction.Response> {

    static final GrokProcessorGetAction INSTANCE = new GrokProcessorGetAction();
    static final String NAME = "cluster:admin/ingest/processor/grok/get";

    private GrokProcessorGetAction() {
        super(NAME);
    }

    public static class Request extends LocalOnlyActionRequest {

        private final boolean sorted;

        public Request(boolean sorted) {
            this.sorted = sorted;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public boolean sorted() {
            return sorted;
        }
    }

    public static class Response extends LocalOnlyActionResponse implements ToXContentObject {
        private final Map<String, String> grokPatterns;

        Response(Map<String, String> grokPatterns) {
            this.grokPatterns = grokPatterns;
        }

        public Map<String, String> getGrokPatterns() {
            return grokPatterns;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("patterns");
            builder.map(grokPatterns);
            builder.endObject();
            return builder;
        }

    }

    public static class TransportAction extends org.opensearch.action.support.TransportAction<Request, Response> {

        private final Map<String, String> grokPatterns;
        private final Map<String, String> sortedGrokPatterns;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            this(transportService.getTaskManager(), actionFilters, Grok.BUILTIN_PATTERNS);
        }

        // visible for testing
        TransportAction(TaskManager taskManager, ActionFilters actionFilters, Map<String, String> grokPatterns) {
            super(NAME, actionFilters, taskManager);
            this.grokPatterns = grokPatterns;
            this.sortedGrokPatterns = new TreeMap<>(this.grokPatterns);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            try {
                listener.onResponse(new Response(request.sorted() ? sortedGrokPatterns : grokPatterns));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return singletonList(new Route(GET, "/_ingest/processor/grok"));
        }

        @Override
        public String getName() {
            return "ingest_processor_grok_get";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            boolean sorted = request.paramAsBoolean("s", false);
            Request grokPatternsRequest = new Request(sorted);
            return channel -> client.executeLocally(INSTANCE, grokPatternsRequest, new RestToXContentListener<>(channel));
        }
    }
}
