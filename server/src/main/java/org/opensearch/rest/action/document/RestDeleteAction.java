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

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.index.VersionType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Transport action to delete a document
 *
 * @opensearch.api
 */
public class RestDeleteAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(DELETE, "/{index}/_doc/{id}")));
    }

    @Override
    public String getName() {
        return "document_delete_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(request.param("index"), request.param("id"));
        deleteRequest.routing(request.param("routing"));
        deleteRequest.timeout(request.paramAsTime("timeout", DeleteRequest.DEFAULT_TIMEOUT));
        deleteRequest.setRefreshPolicy(request.param("refresh"));
        deleteRequest.version(RestActions.parseVersion(request));
        deleteRequest.versionType(VersionType.fromString(request.param("version_type"), deleteRequest.versionType()));
        deleteRequest.setIfSeqNo(request.paramAsLong("if_seq_no", deleteRequest.ifSeqNo()));
        deleteRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", deleteRequest.ifPrimaryTerm()));

        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            deleteRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }

        return channel -> client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }
}
