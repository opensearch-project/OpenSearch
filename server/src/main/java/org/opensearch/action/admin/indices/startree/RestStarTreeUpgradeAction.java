/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for the star tree upgrade action.
 * Registers the {@code POST /{index}/_star_tree/upgrade} route,
 * parses the star tree configuration from the request body,
 * and delegates to {@link TransportStarTreeUpgradeAction} via
 * {@link StarTreeUpgradeAction}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestStarTreeUpgradeAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_star_tree/upgrade"));
    }

    @Override
    public String getName() {
        return "star_tree_upgrade_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        StarTreeField starTreeField = StarTreeUpgradeRequest.parseStarTreeConfig(request.requiredContent(), request.getMediaType());
        StarTreeUpgradeRequest upgradeRequest = new StarTreeUpgradeRequest(indices, starTreeField);
        return channel -> client.execute(StarTreeUpgradeAction.INSTANCE, upgradeRequest, new RestToXContentListener<>(channel));
    }
}
