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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put index template
 *
 * @opensearch.api
 */
public class RestPutIndexTemplateAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutIndexTemplateAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_template/{name}"), new Route(PUT, "/_template/{name}")));
    }

    @Override
    public String getName() {
        return "put_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest(request.param("name"));
        if (request.hasParam("template")) {
            deprecationLogger.deprecate(
                "put_index_template_deprecated_parameter",
                "Deprecated parameter [template] used, replaced by [index_patterns]"
            );
            putRequest.patterns(Collections.singletonList(request.param("template")));
        } else {
            putRequest.patterns(Arrays.asList(request.paramAsStringArray("index_patterns", Strings.EMPTY_ARRAY)));
        }
        putRequest.order(request.paramAsInt("order", putRequest.order()));
        putRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", putRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(putRequest, request, deprecationLogger, getName());
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", ""));

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getMediaType()).v2();
        sourceAsMap = RestCreateIndexAction.prepareMappings(sourceAsMap);
        putRequest.source(sourceAsMap);

        return channel -> client.admin().indices().putTemplate(putRequest, new RestToXContentListener<>(channel));
    }
}
