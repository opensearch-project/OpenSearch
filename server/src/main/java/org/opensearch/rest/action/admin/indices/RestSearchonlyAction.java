package org.opensearch.rest.action.admin.indices;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static java.util.Arrays.asList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestSearchonlyAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "searchonly_index_action";
    }

    @Override
    public List<Route> routes() {
        return asList(new Route(POST, "/{index}/_searchonly/enable"), new Route(POST, "/{index}/_searchonly/disable"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        boolean scaleDown = request.path().endsWith("/enable");

        return channel -> client.admin()
            .indices()
            .prepareSearchOnly(indices)
            .setScaleDown(scaleDown)
            .execute(new RestToXContentListener<>(channel));
    }
}
