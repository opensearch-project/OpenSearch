package org.opensearch.action.admin.indices.searchonly;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class SearchOnlyRequestBuilder extends ActionRequestBuilder<SearchOnlyRequest, AcknowledgedResponse> {

    public SearchOnlyRequestBuilder(OpenSearchClient client, String... indices) {
        this(client, false, indices);
    }

    public SearchOnlyRequestBuilder(OpenSearchClient client, boolean scaleDown, String... indices) {
        super(client, SearchOnlyAction.INSTANCE, new SearchOnlyRequest(indices, scaleDown));
    }

    /**
     * Sets the scale direction (up/down)
     * @param scaleDown true if scaling down, false if scaling up
     * @return this builder
     */
    public SearchOnlyRequestBuilder setScaleDown(boolean scaleDown) {
        request.scaleDown(scaleDown);
        return this;
    }
}
