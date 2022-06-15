/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.template.delete;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Transport request builder for deleting an index template
 *
 * @opensearch.internal
 */
public class DeleteIndexTemplateRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    DeleteIndexTemplateRequest,
    AcknowledgedResponse,
    DeleteIndexTemplateRequestBuilder> {

    public DeleteIndexTemplateRequestBuilder(OpenSearchClient client, DeleteIndexTemplateAction action) {
        super(client, action, new DeleteIndexTemplateRequest());
    }

    public DeleteIndexTemplateRequestBuilder(OpenSearchClient client, DeleteIndexTemplateAction action, String name) {
        super(client, action, new DeleteIndexTemplateRequest(name));
    }

    /**
     * Sets the name of the index template to delete.
     */
    public DeleteIndexTemplateRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }
}
