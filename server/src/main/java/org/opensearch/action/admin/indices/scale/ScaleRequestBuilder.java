package org.opensearch.action.admin.indices.scale;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class ScaleRequestBuilder extends ActionRequestBuilder<PreScaleSyncRequest, AcknowledgedResponse> {

    public ScaleRequestBuilder(OpenSearchClient client, String... indices) {
        this(client, false, indices);
    }

    public ScaleRequestBuilder(OpenSearchClient client, boolean scaleDown, String... indices) {
        super(client, PreScaleSyncAction.INSTANCE, new PreScaleSyncRequest(indices, scaleDown));
    }

    /**
     * Sets the scale direction (up/down)
     * @param scaleDown true if scaling down, false if scaling up
     * @return this builder
     */
    public ScaleRequestBuilder setScaleDown(boolean scaleDown) {
        request.scaleDown(scaleDown);
        return this;
    }
}
