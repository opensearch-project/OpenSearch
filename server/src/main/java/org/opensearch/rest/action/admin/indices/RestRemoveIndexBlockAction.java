/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * REST action to remove an index block
 *
 * @opensearch.api
 */
public class RestRemoveIndexBlockAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(DELETE, "/{index}/_block/{block}"));
    }

    @Override
    public String getName() {
        return "remove_index_block_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RemoveIndexBlockRequest removeIndexBlockRequest = new RemoveIndexBlockRequest(
            IndexMetadata.APIBlock.fromName(request.param("block")),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        removeIndexBlockRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", removeIndexBlockRequest.clusterManagerNodeTimeout())
        );
        removeIndexBlockRequest.timeout(request.paramAsTime("timeout", removeIndexBlockRequest.timeout()));
        removeIndexBlockRequest.indicesOptions(IndicesOptions.fromRequest(request, removeIndexBlockRequest.indicesOptions()));
        return channel -> client.admin().indices().removeBlock(removeIndexBlockRequest, new RestToXContentListener<>(channel));
    }
}
