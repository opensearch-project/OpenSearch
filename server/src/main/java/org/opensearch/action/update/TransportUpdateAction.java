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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.update;

import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Performs the update operation.
 * <p>
 * Deprecated use TransportBulkAction with a single item instead
 *
 * @opensearch.internal
 */
@Deprecated
public class TransportUpdateAction extends HandledTransportAction<UpdateRequest, UpdateResponse> {

    private final TransportBulkAction bulkAction;

    @Inject
    public TransportUpdateAction(TransportService transportService, ActionFilters actionFilters, TransportBulkAction bulkAction) {
        super(UpdateAction.NAME, transportService, actionFilters, UpdateRequest::new);
        this.bulkAction = bulkAction;
    }

    public static void resolveAndValidateRouting(Metadata metadata, String concreteIndex, UpdateRequest request) {
        request.routing((metadata.resolveWriteIndexRouting(request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metadata.routingRequired(concreteIndex)) {
            throw new RoutingMissingException(concreteIndex, request.id());
        }
    }

    @Override
    protected void doExecute(Task task, UpdateRequest request, ActionListener<UpdateResponse> listener) {
        // The following is mostly copied from TransportSingleItemBulkAction
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        bulkRequest.timeout(request.timeout());
        bulkRequest.waitForActiveShards(request.waitForActiveShards());
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        bulkAction.execute(task, bulkRequest, ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
            BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                final UpdateResponse response = bulkItemResponse.getResponse();
                listener.onResponse(response);
            } else {
                listener.onFailure(bulkItemResponse.getFailure().getCause());
            }
        }, listener::onFailure));
    }
}
