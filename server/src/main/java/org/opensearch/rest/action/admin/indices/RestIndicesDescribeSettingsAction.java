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

/** REST endpoint for index setting definitions, separate from configured values. The route lives under the literal
 * {@code _index} segment, which no index name can take (names may not start with an underscore), so it shadows no
 * {@code /{index}/_settings/...} route and {@code GET /_settings/{name}} is untouched. There is no {@code /{index}/}
 * form: definitions do not depend on any index.
 * @opensearch.internal
 */
public class RestIndicesDescribeSettingsAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_index/_settings/_describe"));
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
