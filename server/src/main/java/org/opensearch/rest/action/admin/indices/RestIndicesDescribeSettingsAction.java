/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.settings.describe.IndicesDescribeSettingsAction;
import org.opensearch.action.admin.indices.settings.describe.IndicesDescribeSettingsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/** REST endpoint for index setting definitions, separate from configured values. There is no {@code /{index}/} form:
 * definitions do not depend on any index. The literal {@code _describe} segment takes precedence over
 * {@code /_settings/{name}} in the path trie, as {@code /_snapshot/_status} does beside {@code /_snapshot/{repository}}.
 * @opensearch.internal
 */
public class RestIndicesDescribeSettingsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_settings/_describe"));
    }

    @Override
    public String getName() {
        return "indices_describe_settings_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        var describeRequest = new IndicesDescribeSettingsRequest(request.paramAsStringArray("settings", new String[0]));
        return channel -> client.execute(IndicesDescribeSettingsAction.INSTANCE, describeRequest, new RestToXContentListener<>(channel));
    }
}
