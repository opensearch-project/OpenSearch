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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put stored script
 *
 * @opensearch.api
 */
public class RestPutStoredScriptAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutStoredScriptAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/_scripts/{id}"),
                new Route(PUT, "/_scripts/{id}"),
                new Route(POST, "/_scripts/{id}/{context}"),
                new Route(PUT, "/_scripts/{id}/{context}")
            )
        );
    }

    @Override
    public String getName() {
        return "put_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        String context = request.param("context");
        BytesReference content = request.requiredContent();
        XContentType xContentType = request.getXContentType();
        StoredScriptSource source = StoredScriptSource.parse(content, xContentType);

        PutStoredScriptRequest putRequest = new PutStoredScriptRequest(id, context, content, request.getXContentType(), source);
        putRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", putRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(putRequest, request, deprecationLogger, getName());
        putRequest.timeout(request.paramAsTime("timeout", putRequest.timeout()));
        return channel -> client.admin().cluster().putStoredScript(putRequest, new RestToXContentListener<>(channel));
    }
}
