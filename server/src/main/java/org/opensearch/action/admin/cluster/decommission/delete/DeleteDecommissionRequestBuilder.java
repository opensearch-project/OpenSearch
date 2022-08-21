/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.delete;

import org.opensearch.action.support.master.AcknowledgedRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.decommission.DecommissionAttribute;

public class DeleteDecommissionRequestBuilder extends AcknowledgedRequestBuilder<
        DeleteDecommissionRequest,
        DeleteDecommissionResponse,
        DeleteDecommissionRequestBuilder> {

    public DeleteDecommissionRequestBuilder(OpenSearchClient client, DeleteDecommissionAction action) {
        super(client, action, new DeleteDecommissionRequest());
    }

    /**
     *
     * @param name name of the attribute
     * @return current object
     */
    public DeleteDecommissionRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    /**
     * @param decommissionAttribute decommission attribute
     * @return current object
     */
    public DeleteDecommissionRequestBuilder setDecommissionedAttribute(DecommissionAttribute decommissionAttribute) {
        request.setDecommissionAttribute(decommissionAttribute);
        return this;
    }

}
