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

package org.opensearch.script.mustache;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.script.mustache.TransportSearchTemplateAction.convert;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final NodeClient client;

    @Inject
    public TransportMultiSearchTemplateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client
    ) {
        super(MultiSearchTemplateAction.NAME, transportService, actionFilters, MultiSearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        List<Integer> originalSlots = new ArrayList<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(request.indicesOptions());
        if (request.maxConcurrentSearchRequests() != 0) {
            multiSearchRequest.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[request.requests().size()];
        for (int i = 0; i < items.length; i++) {
            SearchTemplateRequest searchTemplateRequest = request.requests().get(i);
            SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
            SearchRequest searchRequest;
            try {
                searchRequest = convert(searchTemplateRequest, searchTemplateResponse, scriptService, xContentRegistry);
            } catch (Exception e) {
                items[i] = new MultiSearchTemplateResponse.Item(null, e);
                continue;
            }
            items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            if (searchRequest != null) {
                multiSearchRequest.add(searchRequest);
                originalSlots.add(i);
            }
        }

        client.multiSearch(multiSearchRequest, ActionListener.wrap(r -> {
            for (int i = 0; i < r.getResponses().length; i++) {
                MultiSearchResponse.Item item = r.getResponses()[i];
                int originalSlot = originalSlots.get(i);
                if (item.isFailure()) {
                    items[originalSlot] = new MultiSearchTemplateResponse.Item(null, item.getFailure());
                } else {
                    items[originalSlot].getResponse().setResponse(item.getResponse());
                }
            }
            listener.onResponse(new MultiSearchTemplateResponse(items, r.getTook().millis()));
        }, listener::onFailure));
    }
}
