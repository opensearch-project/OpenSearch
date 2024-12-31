package org.opensearch.rest.action.admin.indices;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

public class RestScaleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "scale_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_scale/{direction}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        boolean scaleDown = request.param("direction").equals("down");

        return channel -> client.admin()
            .indices()
            .prepareScale(indices)
            .setScaleDown(scaleDown)
            .execute(new RestToXContentListener<>(channel));
    }
}
