/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.api.action;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.client.OpenSearchClient;

public class APIRequestBuilder extends ActionRequestBuilder<APIRequest, APIResponse> {

    public APIRequestBuilder(OpenSearchClient client, APIAction action) {
        super(client, action, new APIRequest());
    }
}
