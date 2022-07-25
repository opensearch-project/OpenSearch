/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.put;

import org.opensearch.action.support.master.AcknowledgedRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.decommission.DecommissionedAttribute;

/**
 * Builder for a decommission request
 *
 * @opensearch.internal
 */
public class PutDecommissionRequestBuilder extends AcknowledgedRequestBuilder<
    PutDecommissionRequest,
    PutDecommissionResponse,
    PutDecommissionRequestBuilder> {
    public PutDecommissionRequestBuilder(OpenSearchClient client, PutDecommissionAction action) {
        super(client, action, new PutDecommissionRequest());
    }

    /**
     *
     * @param name name of the attribute
     * @return current object
     */
    public PutDecommissionRequestBuilder setName(String name) {
        request.setName(name);
        return this;
    }

    /**
     * @param decommissionedAttribute decommission attribute
     * @return current object
     */
    public PutDecommissionRequestBuilder setDecommissionedAttribute(DecommissionedAttribute decommissionedAttribute) {
        request.setDecommissionAttribute(decommissionedAttribute);
        return this;
    }
}

